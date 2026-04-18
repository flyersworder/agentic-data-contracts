"""Set up a sample DuckDB database for the ops reliability agent example."""

import duckdb


def setup(db_path: str = "sample_data.duckdb") -> None:
    conn = duckdb.connect(db_path)
    conn.execute("CREATE SCHEMA IF NOT EXISTS sre")

    conn.execute("""
        CREATE OR REPLACE TABLE sre.services (
            id INTEGER,
            name VARCHAR,
            tier VARCHAR,
            owner_team VARCHAR,
            tenant_id VARCHAR
        )
    """)
    conn.execute("""
        INSERT INTO sre.services VALUES
        (1, 'checkout-api',      'tier-1', 'payments',    'acme'),
        (2, 'auth-service',      'tier-1', 'platform',    'acme'),
        (3, 'search',            'tier-2', 'discovery',   'acme'),
        (4, 'recommendations',   'tier-2', 'ml-infra',    'acme'),
        (5, 'notification-mail', 'tier-3', 'growth',      'acme')
    """)

    # SLA targets (minutes): SEV1=60, SEV2=240, SEV3=1440.
    # `resolved_within_sla` is materialized at write time to keep metric SQL simple.
    conn.execute("""
        CREATE OR REPLACE TABLE sre.incidents (
            id INTEGER,
            service_id INTEGER,
            severity VARCHAR,
            opened_at TIMESTAMP,
            resolved_at TIMESTAMP,
            cause VARCHAR,
            user_email VARCHAR,
            customer_id VARCHAR,
            resolved_within_sla BOOLEAN,
            tenant_id VARCHAR
        )
    """)
    conn.execute("""
        INSERT INTO sre.incidents VALUES
        (101, 1, 'SEV1', '2026-04-15 02:14', '2026-04-15 02:58', 'db-connection-pool-exhaust',     'u1@ex.com',  'C-042', TRUE,  'acme'),
        (102, 2, 'SEV2', '2026-04-15 11:20', '2026-04-15 14:05', 'oauth-provider-rate-limit',      'u2@ex.com',  'C-109', TRUE,  'acme'),
        (103, 3, 'SEV3', '2026-04-16 08:30', '2026-04-16 19:00', 'stale-index',                     NULL,         NULL,    TRUE,  'acme'),
        (104, 1, 'SEV1', '2026-04-17 03:55', '2026-04-17 05:45', 'bad-deploy-rollback',            'u3@ex.com',  'C-087', FALSE, 'acme'),
        (105, 4, 'SEV2', '2026-04-17 14:10', '2026-04-17 17:30', 'model-inference-timeout',         NULL,         NULL,    TRUE,  'acme'),
        (106, 2, 'SEV1', '2026-04-18 01:00', '2026-04-18 01:45', 'cert-expiry',                     'u5@ex.com',  'C-221', TRUE,  'acme'),
        (107, 5, 'SEV3', '2026-04-18 09:00', NULL,                'smtp-backlog',                    NULL,         NULL,    NULL,  'acme')
    """)

    conn.execute("""
        CREATE OR REPLACE TABLE sre.deploys (
            id INTEGER,
            service_id INTEGER,
            commit_sha VARCHAR,
            deployed_at TIMESTAMP,
            deployed_by VARCHAR,
            success BOOLEAN,
            tenant_id VARCHAR
        )
    """)
    conn.execute("""
        INSERT INTO sre.deploys VALUES
        (1001, 1, 'a1b2c3d4', '2026-04-14 10:00', 'alice@acme', TRUE,  'acme'),
        (1002, 1, 'e5f6a7b8', '2026-04-15 09:30', 'bob@acme',   TRUE,  'acme'),
        (1003, 1, 'c9d0e1f2', '2026-04-17 03:20', 'carol@acme', FALSE, 'acme'),
        (1004, 2, 'f3a4b5c6', '2026-04-16 14:00', 'alice@acme', TRUE,  'acme'),
        (1005, 3, 'd7e8f9a0', '2026-04-13 11:15', 'dan@acme',   TRUE,  'acme'),
        (1006, 4, 'b1c2d3e4', '2026-04-16 16:45', 'eve@acme',   TRUE,  'acme'),
        (1007, 5, 'f5a6b7c8', '2026-04-12 10:30', 'frank@acme', TRUE,  'acme')
    """)
    conn.close()
    print(f"Sample database created at {db_path}")


if __name__ == "__main__":
    setup()
