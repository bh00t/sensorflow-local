"""
DuckDB Role-Based Access Control (RBAC) Simulator

This module acts as a lightweight Data Governance layer. Because DuckDB is an
in-process database (unlike Snowflake or Redshift), it does not have built-in 
user roles. We handle security at the Python application layer before any SQL 
is actually sent to the database engine.

Security Features:
1. Table-Level Security: Blocks access to unauthorized tables.
2. Column-Level Security: Scans SQL to block access to sensitive/audit columns.
"""

import duckdb

# Database path (Ensure other writers are closed when running this)
DB_PATH = 'data\\sensorflow.db'

# ─── GOVERNANCE DICTIONARY ───────────────────────────────────────────────────
# This defines the exact permissions for each role. In a real enterprise 
# application, this might be loaded from a YAML file or an AWS IAM policy.
ROLES = {
    'engineer': {
        'allowed_tables': [
            'fact_sensor_readings', 'dim_machine', 'dim_sensor',
            'dim_location', 'dim_date', 'anomaly_log', 'hourly_machine_summary'
        ],
        'denied_columns': [],   # God-mode: Can see all audit/metadata columns
        'description': 'Full pipeline access — all tables, all columns'
    },
    'analyst': {
        'allowed_tables': [
            'fact_sensor_readings', 'dim_machine', 'dim_sensor',
            'dim_location', 'dim_date', 'hourly_machine_summary'
        ],
        # Column-Level Security: Analysts cannot see internal pipeline timestamps
        'denied_columns': ['_ingested_at', '_loaded_at'],  
        'description': 'Business queries only — restricted from audit columns'
    },
    'viewer': {
        # Table-Level Security: Viewers are blocked from heavy RAW tables
        'allowed_tables': ['hourly_machine_summary', 'dim_machine'],  
        'denied_columns': [],
        'description': 'Read-only summaries — no raw fact table access'
    },
}

# ─── CORE FUNCTIONS ──────────────────────────────────────────────────────────

def get_connection(role_name: str) -> duckdb.DuckDBPyConnection:
    """
    Validates the requested role and returns a read-only database connection.
    
    Args:
        role_name (str): The role attempting to connect (e.g., 'analyst').
    Returns:
        duckdb.DuckDBPyConnection: A secure, read-only connection.
    """
    if role_name not in ROLES:
        raise ValueError(f"Security Alert: Unknown role '{role_name}'. Valid roles: {list(ROLES.keys())}")
    
    # Always open in read_only mode for query/analytics users to prevent data mutation
    return duckdb.connect(DB_PATH, read_only=True)


def role_query(con: duckdb.DuckDBPyConnection, role_name: str, table: str, sql: str):
    """
    Intercepts the SQL query and enforces Table and Column level security rules 
    before sending it to the DuckDB engine.
    
    Args:
        con: The active DuckDB connection.
        role_name: The role executing the query.
        table: The primary table being queried.
        sql: The raw SQL string to execute.
    Returns:
        pandas.DataFrame: The query results if permissions pass.
    Raises:
        PermissionError: If the role violates table or column restrictions.
    """
    role_config = ROLES[role_name]
    
    # 1. Enforce Table-Level Security
    if table not in role_config['allowed_tables']:
        raise PermissionError(
            f"ACCESS DENIED: Role '{role_name}' is not authorized to query the '{table}' table."
        )
        
    # 2. Enforce Column-Level Security
    # We do a basic string search to see if the denied column exists in the SQL
    for col in role_config['denied_columns']:
        if col.lower() in sql.lower():
            raise PermissionError(
                f"ACCESS DENIED: Role '{role_name}' is restricted from viewing the '{col}' column."
            )
            
    # If both security checks pass, execute the query and return a Pandas DataFrame
    return con.execute(sql).df()

# ─── TEST SUITE ──────────────────────────────────────────────────────────────

if __name__ == '__main__':
    # We will simulate all three users logging in and trying to run queries
    for role_name in ['engineer', 'analyst', 'viewer']:
        print(f"\n{'='*40}")
        print(f"👤 LOGIN: {role_name.upper()}")
        print(f"📝 {ROLES[role_name]['description']}")
        print(f"{'='*40}")
        
        # Initialize connection for this specific user
        con = get_connection(role_name)
        
        # Test 1: Querying the massive RAW Fact table
        print("\n[Test 1] Attempting to read RAW Fact Table...")
        try:
            df = role_query(con, role_name, 'fact_sensor_readings',
                'SELECT machine_id, COUNT(*) AS n FROM fact_sensor_readings GROUP BY 1')
            print(f"✅ SUCCESS: Returned {len(df)} aggregated rows.")
        except PermissionError as e:
            print(f"❌ BLOCKED: {e}")
            
        # Test 2: Querying restricted pipeline audit columns
        print("\n[Test 2] Attempting to read internal audit columns...")
        try:
            role_query(con, role_name, 'fact_sensor_readings',
                'SELECT machine_id, _ingested_at FROM fact_sensor_readings LIMIT 3')
            print("✅ SUCCESS: Data retrieved.")
        except PermissionError as e:
            print(f"❌ BLOCKED: {e}")
            
        # Test 3: Querying the lightweight summary table
        print("\n[Test 3] Attempting to read Hourly Summaries...")
        try:
            df = role_query(con, role_name, 'hourly_machine_summary',
                'SELECT machine_id, AVG(avg_value) FROM hourly_machine_summary GROUP BY 1')
            print(f"✅ SUCCESS: Returned {len(df)} summary rows.")
        except PermissionError as e:
            print(f"❌ BLOCKED: {e}")
            
        # Close connection to free up the file lock for the next user
        con.close()