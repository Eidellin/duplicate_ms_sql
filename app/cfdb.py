import os
import sys
import pyodbc
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
from helper.main import deny_access

def get_tables(env, con):
    tables = []
    QUERY_PATH = BASE_DIR / '../sql/getTables.sql'
    SQL_QUERY = open(QUERY_PATH, 'r').read().format(DATABASE=env['DATABASE'])

    cursor = con.cursor()
    cursor.execute(SQL_QUERY)
    records = cursor.fetchall()
    
    for record in records:
        tables.append(record[0])
    
    return tables

def get_columns(env, con, TABLE):
    """('COLUMN_NAME', 'DATA_TYPE', 'IS_NULLABLE', 'COLUMN_DEFAULT')"""
    columns = []
    QUERY_PATH = BASE_DIR / '../sql/getColumns.sql'
    SQL_QUERY = open(QUERY_PATH, 'r').read().format(DATABASE=env['DATABASE'], TABLE=TABLE)

    cursor = con.cursor()
    cursor.execute(SQL_QUERY)
    records = cursor.fetchall()
    
    for record in records:
        columns.append(record)
    return columns

def get_table_data(env, con, TABLE, paste_to, pcon):
    DATABASE, OWNER = env['DATABASE'], env['OWNER']
    QUERY_PATH = BASE_DIR / '../sql/getTableData.sql'
    SQL_QUERY = open(QUERY_PATH, 'r').read().format(DATABASE=env['DATABASE'], OWNER=env['OWNER'], TABLE=TABLE)
    
    cursor = con.cursor()
    try:
        cursor.execute(SQL_QUERY)
        records = cursor.fetchall()
        
        data = [(column[0] for column in cursor.description)]
        
        for record in records:
            data.append(record)
        return data
    except pyodbc.ProgrammingError as e:
        if "The SELECT permission was denied on the column" in str(e):
            # print(f"Table {TABLE} is denied to be read.")
            # with open(BASE_DIR / '../care/denyTable.txt', 'a') as f:
                # f.write(f"{TABLE}\n")
            deny_access(paste_to, pcon, TABLE)
            return []

def get_alter_constraints_scripts(env, con):
    scripts = []
    QUERY_PATH = BASE_DIR / '../sql/getAlterConstraintsScripts.sql'
    SQL_QUERY = open(QUERY_PATH, 'r').read().format(DATABASE=env['DATABASE'])
    cursor = con.cursor()
    try:
        cursor.execute(SQL_QUERY)
    except pyodbc.ProgrammingError as e:
        print(SQL_QUERY)
        raise e
    results = cursor.fetchall()
    for result in results:
        scripts.append(str(result[0]))
    return scripts
