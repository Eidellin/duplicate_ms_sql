import os
import pyodbc
from pathlib import Path
from dotenv import load_dotenv
from cfdb import get_tables, get_columns, get_table_data, get_alter_constraints_scripts
from ptdb import create_table, fill_table, apply_constraints, reset_constraints
from dbo import clearDB, resetDB
from tqdm import tqdm

BASE_DIR = Path(__file__).resolve().parent

def env():
    load_dotenv()
    
    env_dict = {
        "c": dict(),
        "p": dict()
    }
    
    for k in env_dict.keys():
        env = k
        
        owner = os.getenv(f"{env}.OWNER")
        
        env_dict[k] = {
            "SERVER": os.getenv(f"{env}.SERVER"),
            "DATABASE": os.getenv(f"{env}.DATABASE"),
            "USERNAME": os.getenv(f"{env}.USERNAME"),
            "PASSWORD": os.getenv(f"{env}.PASSWORD"),
            "OWNER": owner if owner else 'dbo'
        }
        
    return env_dict

def connection(env):
    SERVER, DATABASE, USERNAME, PASSWORD, OWNER = env.values()
    connectionString = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD}'
    return pyodbc.connect(connectionString)

def main():
    copy_from, paste_to = env().values()
    ccon = connection(copy_from)
    pcon = connection(paste_to)
    
    clearDB(paste_to, pcon, get_tables(paste_to, pcon))
    tables = get_tables(copy_from, ccon)
    for _, table in enumerate(tqdm(tables, desc="Processing Tables", unit="table")):
        columns = get_columns(copy_from, ccon, table)
        create_table(paste_to, pcon, columns, table)
        data = get_table_data(copy_from, ccon, table, paste_to, pcon)
        fill_table(paste_to, pcon, data, table)

    # reset_constraints(paste_to, pcon, tables)
    scripts = get_alter_constraints_scripts(copy_from, ccon)
    apply_constraints(paste_to, pcon, scripts)
    
if __name__ == "__main__":
    main()
