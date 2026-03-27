"""Set up a sample DuckDB database for the revenue agent example."""

import duckdb


def setup(db_path: str = "sample_data.duckdb") -> None:
    conn = duckdb.connect(db_path)
    conn.execute("CREATE SCHEMA IF NOT EXISTS analytics")
    conn.execute("""
        CREATE OR REPLACE TABLE analytics.customers (
            id INTEGER, name VARCHAR, region VARCHAR, tenant_id VARCHAR
        )
    """)
    conn.execute("""
        INSERT INTO analytics.customers VALUES
        (1, 'Alice Corp', 'North America', 'acme'),
        (2, 'Bob Ltd', 'Europe', 'acme'),
        (3, 'Charlie Inc', 'Asia Pacific', 'acme'),
        (4, 'Diana GmbH', 'Europe', 'acme'),
        (5, 'Eve SA', 'North America', 'acme')
    """)
    conn.execute("""
        CREATE OR REPLACE TABLE analytics.orders (
            id INTEGER, customer_id INTEGER, amount DECIMAL(10,2),
            status VARCHAR, tenant_id VARCHAR, created_at DATE
        )
    """)
    conn.execute("""
        INSERT INTO analytics.orders VALUES
        (1, 1, 1500.00, 'completed', 'acme', '2025-01-15'),
        (2, 2, 2300.00, 'completed', 'acme', '2025-01-20'),
        (3, 3, 800.00, 'completed', 'acme', '2025-02-01'),
        (4, 1, 1200.00, 'completed', 'acme', '2025-02-15'),
        (5, 4, 3100.00, 'completed', 'acme', '2025-03-01'),
        (6, 5, 950.00, 'pending', 'acme', '2025-03-10'),
        (7, 2, 1800.00, 'completed', 'acme', '2025-03-15'),
        (8, 3, 2100.00, 'cancelled', 'acme', '2025-03-20')
    """)
    conn.execute("""
        CREATE OR REPLACE TABLE analytics.subscriptions (
            id INTEGER, customer_id INTEGER, plan VARCHAR, tenant_id VARCHAR
        )
    """)
    conn.execute("""
        INSERT INTO analytics.subscriptions VALUES
        (1, 1, 'enterprise', 'acme'),
        (2, 2, 'pro', 'acme'),
        (3, 3, 'starter', 'acme')
    """)
    conn.close()
    print(f"Sample database created at {db_path}")


if __name__ == "__main__":
    setup()
