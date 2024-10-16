from pathlib import Path
from datetime import datetime

BASE_DIR = Path(__file__).resolve().parent

# Helper function to format values safely
def safe_format(value):
    if value is None:
        return "NULL"  # Convert None to SQL NULL
    elif isinstance(value, datetime):
        return f"'{value.strftime('%Y-%m-%d %H:%M:%S')}'"  # Format datetime to SQL string
    elif isinstance(value, str):
        value = value.replace("'", "''")
        return f"'{value}'"  # Wrap strings in single quotes
    elif isinstance(value, bool):
        return str(int(value))  # Convert booleans to integers
    elif isinstance(value, (bytes, bytearray)):
        # Format binary data for SQL Server as hexadecimal, prefixed with 0x
        return f"0x{value.hex()}"
    else:
        return str(value)  # Convert other types to strings

def count_rows(env, con, table):
    QUERY_PATH = BASE_DIR / '../sql/countRows.sql'
    SQL_QUERY = open(QUERY_PATH, 'r').read().format(TABLE=table)
    cursor = con.cursor()
    cursor.execute(SQL_QUERY)
    # result = int(cursor.fetchone()[0])
    result = int(cursor.fetchone()[0])
    
    return result

def deny_access(env, con, TABLE):
    OWNER = env['OWNER']
    SQL_QUERY = f"DENY SELECT ON {OWNER}.{TABLE} TO Public;"

    cursor = con.cursor()
    cursor.execute(SQL_QUERY)
    con.commit()
    return "Access Denied for Table: " + TABLE

def constraint_type(CONSTRAINT_CODE):
    CONSTRAINT_CODE = CONSTRAINT_CODE.upper()
    CONSTRAINT_TYPE = 'PRIMARY KEY' if CONSTRAINT_CODE == 'PK' else 'FOREIGN KEY' if CONSTRAINT_CODE == 'FK' else 'UNIQUE' if CONSTRAINT_CODE == 'UQ' else 'CHECK'
    return CONSTRAINT_TYPE