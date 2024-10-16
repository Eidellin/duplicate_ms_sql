import os
import sys
from pathlib import Path
from errors.auth import DBOError
from cfdb import get_tables

BASE_DIR = Path(__file__).resolve().parent

current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
from helper.main import count_rows

def resetDB(env, con):
    DATABASE, OWNER = env['DATABASE'], env['OWNER']
    if OWNER != 'dbo':
        raise DBOError

    QUERY_PATH = BASE_DIR / '../sql/resetDB.sql'
    SQL_QUERY = open(QUERY_PATH, 'r').read()
    
    cursor = con.cursor()
    cursor.execute(SQL_QUERY)
    con.commit()

    result = len(get_tables(env, con)) == 0
    return result
    
def clearDB(env, con, tables):
    DATABASE, OWNER = env['DATABASE'], env['OWNER']
    if OWNER != 'dbo':
        raise DBOError

    QUERY_PATH = BASE_DIR / '../sql/clearDB.sql'
    SQL_QUERY = open(QUERY_PATH, 'r').read()
    
    cursor = con.cursor()
    cursor.execute(SQL_QUERY)
    
    con.commit()

    count = 0
    for table in tables:
        count += count_rows(env, con, table)
    return count == 0
