"""Set up a sample DuckDB database for the growth agent example.

Three tables:
- users (signup timeline + PII columns to exercise the `log` pii_exposure_audit rule)
- events (event stream with experiment tagging)
- experiments (experiment metadata — running vs concluded)
"""

import duckdb


def setup(db_path: str = "sample_data.duckdb") -> None:
    conn = duckdb.connect(db_path)
    conn.execute("CREATE SCHEMA IF NOT EXISTS analytics")

    conn.execute("""
        CREATE OR REPLACE TABLE analytics.users (
            id INTEGER,
            signup_at DATE,
            acquisition_source VARCHAR,
            email VARCHAR,
            phone VARCHAR,
            tenant_id VARCHAR
        )
    """)
    # Users 1-5 signed up during onboarding-042 (2025-Q3, concluded);
    # users 6-10 during pricing-019 (2026-Q1, still running).
    conn.execute("""
        INSERT INTO analytics.users VALUES
        (1, '2025-07-15', 'paid_search',    'alice@ex.com',   '+1-555-0001', 'acme'),
        (2, '2025-07-22', 'organic',        'bob@ex.com',     '+1-555-0002', 'acme'),
        (3, '2025-08-05', 'referral',       'charlie@ex.com', '+1-555-0003', 'acme'),
        (4, '2025-08-18', 'paid_search',    'diana@ex.com',   '+1-555-0004', 'acme'),
        (5, '2025-09-10', 'social',         'eve@ex.com',     '+1-555-0005', 'acme'),
        (6, '2026-04-08', 'organic',        'frank@ex.com',   '+1-555-0006', 'acme'),
        (7, '2026-04-10', 'paid_search',    'grace@ex.com',   '+1-555-0007', 'acme'),
        (8, '2026-04-12', 'referral',       'henry@ex.com',   '+1-555-0008', 'acme'),
        (9, '2026-04-14', 'social',         'iris@ex.com',    '+1-555-0009', 'acme'),
        (10,'2026-04-15', 'paid_search',    'jane@ex.com',    '+1-555-0010', 'acme')
    """)

    conn.execute("""
        CREATE OR REPLACE TABLE analytics.experiments (
            id VARCHAR,
            name VARCHAR,
            status VARCHAR,
            start_date DATE,
            end_date DATE
        )
    """)
    conn.execute("""
        INSERT INTO analytics.experiments VALUES
        ('onboarding-042', 'Streamlined onboarding flow',       'concluded', '2025-07-01', '2025-09-30'),
        ('onboarding-051', 'Onboarding flow v3',                 'concluded', '2025-11-01', '2025-12-15'),
        ('pricing-019',    'Self-serve pricing experiment',      'running',   '2026-03-15', NULL)
    """)

    # Two user FKs: `user_id` (the actor) and `referrer_user_id` (who invited
    # them, when acquisition_source = 'referral'). The semantic contract marks
    # `user_id` as the canonical join (preferred: true) so the agent uses it by
    # default for analytics, and reaches for `referrer_user_id` only when a
    # question is specifically about referral mechanics.
    conn.execute("""
        CREATE OR REPLACE TABLE analytics.events (
            id INTEGER,
            user_id INTEGER,
            referrer_user_id INTEGER,
            event_name VARCHAR,
            experiment_id VARCHAR,
            variant VARCHAR,
            spend_usd DECIMAL(10,2),
            created_at TIMESTAMP,
            tenant_id VARCHAR
        )
    """)
    # Each user has a signup event; some activate; fewer convert.
    # Users 1-5 are in onboarding-042 (concluded); 6-10 in pricing-019 (running).
    # User 3 (charlie) and user 8 (henry) signed up via referral — referrer set.
    conn.execute("""
        INSERT INTO analytics.events VALUES
        -- signups (all users)
        (1,  1,  NULL, 'signup',         NULL,             NULL,        0,    '2025-07-15 10:01', 'acme'),
        (2,  2,  NULL, 'signup',         NULL,             NULL,        0,    '2025-07-22 14:22', 'acme'),
        (3,  3,  1,    'signup',         NULL,             NULL,        0,    '2025-08-05 09:15', 'acme'),
        (4,  4,  NULL, 'signup',         NULL,             NULL,        0,    '2025-08-18 11:40', 'acme'),
        (5,  5,  NULL, 'signup',         NULL,             NULL,        0,    '2025-09-10 16:03', 'acme'),
        (6,  6,  NULL, 'signup',         NULL,             NULL,        0,    '2026-04-08 08:55', 'acme'),
        (7,  7,  NULL, 'signup',         NULL,             NULL,        0,    '2026-04-10 13:12', 'acme'),
        (8,  8,  4,    'signup',         NULL,             NULL,        0,    '2026-04-12 17:45', 'acme'),
        (9,  9,  NULL, 'signup',         NULL,             NULL,        0,    '2026-04-14 10:30', 'acme'),
        (10, 10, NULL, 'signup',         NULL,             NULL,        0,    '2026-04-15 15:20', 'acme'),
        -- activations (concluded experiment: onboarding-042, users 1-5)
        (11, 1,  NULL, 'activation',     'onboarding-042', 'new_flow',  0,    '2025-07-15 10:12', 'acme'),
        (12, 2,  NULL, 'activation',     'onboarding-042', 'new_flow',  0,    '2025-07-24 09:02', 'acme'),
        (13, 3,  1,    'activation',     'onboarding-042', 'control',   0,    '2025-08-07 14:18', 'acme'),
        (14, 4,  NULL, 'activation',     'onboarding-042', 'new_flow',  0,    '2025-08-20 11:55', 'acme'),
        -- user 5 did not activate
        -- activations (running experiment: pricing-019, users 6-10)
        (15, 6,  NULL, 'activation',     'pricing-019',    'A',         0,    '2026-04-09 09:30', 'acme'),
        (16, 7,  NULL, 'activation',     'pricing-019',    'B',         0,    '2026-04-11 13:40', 'acme'),
        -- henry (user 8) signed up via referral; the FK propagates through his
        -- whole event history, illustrating the referrer-edge join target.
        (17, 8,  4,    'activation',     'pricing-019',    'B',         0,    '2026-04-13 18:20', 'acme'),
        (18, 9,  NULL, 'activation',     'pricing-019',    'A',         0,    '2026-04-15 10:50', 'acme'),
        -- user 10 did not activate
        -- purchases (conversion events)
        (19, 1,  NULL, 'first_purchase', 'onboarding-042', 'new_flow',  0,    '2025-08-12 10:00', 'acme'),
        (20, 2,  NULL, 'first_purchase', 'onboarding-042', 'new_flow',  0,    '2025-08-25 11:00', 'acme'),
        (21, 6,  NULL, 'first_purchase', 'pricing-019',    'A',         0,    '2026-04-17 08:45', 'acme'),
        -- marketing spend records (one row per week, placeholder shape)
        (22, NULL,NULL, 'spend',         NULL,             NULL,        1200, '2026-03-23 00:00', 'acme'),
        (23, NULL,NULL, 'spend',         NULL,             NULL,        1500, '2026-03-30 00:00', 'acme'),
        (24, NULL,NULL, 'spend',         NULL,             NULL,        1800, '2026-04-06 00:00', 'acme'),
        (25, NULL,NULL, 'spend',         NULL,             NULL,        2100, '2026-04-13 00:00', 'acme')
    """)
    conn.close()
    print(f"Sample database created at {db_path}")


if __name__ == "__main__":
    setup()
