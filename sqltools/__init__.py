import os
import pyodbc
import pandas as pd
import numpy as np
from sqlalchemy.engine import URL, create_engine

DB_CONN = os.getenv("DB_CONN")
if not DB_CONN: raise Exception("DB_CONN not set in .env! Read the 'Setting secrets' section in the README.")


#=============#
#   Loaders   #
#=============#
# Load a CSV into SQL
def sql_loader(local_fn, task, if_exists = "append", encoding = "utf-8", ignore_errors = False):
    task_name = task.task_name
    table_name = task.table_name
    schema = task.schema
    database = task.database
    df = pd.read_csv(local_fn, encoding = encoding)
    print(f"Loading {len(df)} rows from dataset '{task_name}'...")
    df["task_name"] = task_name
    usable_cols = check_columns(df, table_name, schema, database, ignore_errors)
    engine = sqlalchemy_engine(database)
    df[usable_cols].to_sql(table_name, engine, schema, if_exists, index = False, chunksize = 1000)
    return len(df)

# Load a dataset row by row to debug type errors
def sql_debug_loader(local_fn, task, if_exists = "append", encoding = "utf-8"):
    task_name = task.task_name
    table_name = task.table_name
    schema = task.schema
    database = task.database
    df = pd.read_csv(local_fn, encoding = encoding)
    print(f"TESTING ONLY: Fake loading {len(df)} rows from dataset '{task_name}'...")
    df["task_name"] = f"debug_{task_name}"
    usable_cols = check_columns(df, table_name, schema, database, ignore_errors = True)
    # Load
    conn = pyodbc_conn(database)
    cur = conn.cursor()
    df = df.replace({ np.nan: None })
    for i,row in df[usable_cols].iterrows():
        # Try to insert the row, if it works, then it's fine
        try:
            query = f"INSERT INTO [{schema}].[{table_name}]({','.join(usable_cols)}) VALUES ({','.join(['?'] * len(usable_cols))})"
            cur.execute(query, *row)
            continue
        # If it fails, do each value one by one until you find the problem
        except:
            for k,v in row.items():
                try:
                    query = f"INSERT INTO [{schema}].[{table_name}]({k}) VALUES (?)"
                    cur.execute(query, v)
                except:
                    print(f"{k}: {v}")
                    raise
            else:
                print(row)
                print("Row failed to load, but all the individual values loaded??")
                raise
    cur.rollback()


#==================#
#   pyodbc-based   #
#==================#
def pyodbc_conn(database):
    conn = pyodbc.connect(f"{DB_CONN};Database={database};")
    return conn

def run_query(query, database, mode):
    conn = pyodbc_conn(database)
    cur = conn.cursor()
    cur.execute(query)
    if mode == "read":
        return cur
    elif mode == "write":
        cur.commit()
        return cur
    elif mode == "test":
        cur.rollback()
        return cur
    else:
        raise Exception("Mode must be 'read', 'write', or 'test'!")

def query_to_df(query, database):
    print("Executing query...")
    cur = run_query(query, database, mode = "read")
    print("Extracting results...")
    cols = [c[0] for c in cur.description]
    raw = [dict(zip(cols, r)) for r in cur.fetchall()]
    df = pd.DataFrame(raw)
    print(f"{len(df)} rows in results...")
    return df


#======================#
#   sqlalchemy-based   #
#======================#
def sqlalchemy_engine(database, fast_executemany = True):
    conn_config = { "odbc_connect": f"{DB_CONN};Database={database};" }
    conn_url = URL.create("mssql+pyodbc", query = conn_config)
    return create_engine(conn_url, fast_executemany = fast_executemany)


#=============#
#   Columns   #
#=============#
def get_columns(table, schema, database):
    query = f"SELECT TOP(1) * FROM [{schema}].[{table}]"
    cur = run_query(query, database, mode = "read")
    return [d[0] for d in cur.description]

def check_columns(df, table, schema, database, ignore_errors = False):
    expected_cols = get_columns(table, schema, database)
    actual_cols = list(df.columns)
    missing_cols = [c for c in expected_cols if c not in actual_cols]
    extra_cols = [c for c in actual_cols if c not in expected_cols]
    usable_cols = [c for c in actual_cols if c in expected_cols]
    if missing_cols:
        print(f"Missing columns: {missing_cols}")
        if not ignore_errors: raise Exception(f"Expected columns are missing!")
    if extra_cols:
        print(f"Unexpected columns: {extra_cols}")
        if not ignore_errors: raise Exception(f"Unexpected columns are present!")
    return usable_cols
