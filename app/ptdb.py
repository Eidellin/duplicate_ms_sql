import os
import sys
import pyodbc
from pathlib import Path
from cfdb import get_columns
from datetime import datetime
from tqdm import tqdm

BASE_DIR = Path(__file__).resolve().parent

current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)

from helper.main import safe_format, constraint_type

CONSTRAINT_CODES = ['PK', 'FK', 'UQ', 'CK']

def create_table(env, con, columns, TABLE):
    DATABASE, OWNER = env['DATABASE'], env['OWNER']

    QUERY_PATH = BASE_DIR / '../sql/createTable.sql'

    COLUMNS = ""
    for column in columns:
        COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT = column
        
        COLUMNS += f"\t[{COLUMN_NAME}]\t{DATA_TYPE}\t"
        COLUMNS += f"CONSTRAINT DEFAULT_{DATABASE}_[{COLUMN_NAME}] DEFAULT {COLUMN_DEFAULT}\t" if COLUMN_DEFAULT is not None else ""
        COLUMNS += f"{IS_NULLABLE},\n"

    SQL_QUERY = open(QUERY_PATH, 'r').read().format(DATABASE=env['DATABASE'], OWNER=env['OWNER'], TABLE=TABLE, COLUMNS=COLUMNS)

    cursor = con.cursor()
    
    try:
        cursor.execute(SQL_QUERY)
        con.commit()
    except pyodbc.ProgrammingError as e:
        if "There is already an object named" in str(e):
            pass
        else:
            print(SQL_QUERY)
            raise e
    
    results = "Columns are:\n", get_columns(env, con, TABLE)
    
    return results

def fill_table(env, con, data, TABLE):
    if data == []:
        return 0
    
    COLUMNS = ", ".join(data[0])
    
    QUERY_PATH = BASE_DIR / '../sql/fillTable.sql'
    
    cursor = con.cursor()
    
    for start in range(1, len(data), 1000):
        end = start + 1000
        
        VALUES = ",\n\t".join(
            f"({', '.join(safe_format(value) for value in row)})" for row in data[start:end]
        )
        
        SQL_QUERY = open(QUERY_PATH, 'r').read().format(OWNER=env['OWNER'], TABLE=TABLE, COLUMNS=COLUMNS, VALUES=VALUES)
        try:
            cursor.execute(SQL_QUERY)
        # except pyodbc.ProgrammingError as e:
        except Exception as e:
            print(SQL_QUERY)
            raise e
        
    con.commit()

    # show result
    QUERY_PATH = BASE_DIR / '../sql/countRows.sql'
    SQL_QUERY = open(QUERY_PATH, 'r').read().format(TABLE=TABLE)
    cursor = con.cursor()
    cursor.execute(SQL_QUERY)
    row_count = cursor.fetchone()[0]
    result = f"Number of rows: {row_count}"
    
    return result

def get_all_constraints(env, con):
    cursor = con.cursor()
    QUERY_PATH = BASE_DIR / '../sql/getAllConstraints.sql'
    SQL_QUERY = open(QUERY_PATH, 'r').read().format(DATABASE=env['DATABASE'])
    cursor.execute(SQL_QUERY)
    records = cursor.fetchall()
    results = []
    for record in records:
        results.append(record)
    return results

def apply_constraints(env, con, constraints):
    cursor = con.cursor()
    for _, script in enumerate(tqdm(constraints, desc="Processing Constraints", unit="script")):
        if script == 'None':
            continue
        try:
            cursor.execute(script)
        except pyodbc.ProgrammingError as e:
            if "already has" in str(e) and "defined on it." in str(e):
                pass
            else:
                print(script)
                raise e
    con.commit()
    return get_all_constraints(env, con)

def reset_constraints(env, con, tables):
    DATABASE, OWNER = env['DATABASE'], env['OWNER']
    QUERY_PATH = BASE_DIR / '../sql/resetConstraint.sql'
    
    cursor = con.cursor()
    
    for _, TABLE in enumerate(tqdm(tables, desc="Resetting Constraints", unit="TABLE")):
        for CONSTRAINT_CODE in CONSTRAINT_CODES:
            SQL_QUERY = open(QUERY_PATH, 'r').read().format(DATABASE=env['DATABASE'], OWNER=env['OWNER'], TABLE=TABLE, CONSTRAINT_CODE=CONSTRAINT_CODE)
            try:
                cursor.execute(SQL_QUERY)
            except pyodbc.ProgrammingError as e:
                if "is not a constraint" in str(e):
                    pass
                else:
                    print(SQL_QUERY)
                    raise e
    
    con.commit()
    return get_all_constraints(env, con)
