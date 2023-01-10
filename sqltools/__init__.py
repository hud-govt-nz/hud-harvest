import os, sys, csv
import struct
import pyodbc
import math, itertools
import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import event
from sqlalchemy.engine import URL, create_engine
from azure.identity import AzureCliCredential

csv.field_size_limit(sys.maxsize)
DB_CONN = os.getenv("DB_CONN")
if not DB_CONN: raise Exception("DB_CONN not set in .env! Read the 'Setting secrets' section in the README.")


#=============#
#   Loaders   #
#=============#
# Load a CSV into SQL
def sql_loader(local_fn, task, if_exists = "append", encoding = "utf-8", strict_mode = True, batch_size = 1000):
    if batch_size > 10000:
        print("\033[1;33mCAUTION! batch_size > 10000 is not recommended!\033[0m")
    task_name = task.task_name
    table_name = task.table_name
    schema = task.schema
    database = task.database
    # Initialise database
    conn = pyodbc_conn(database)
    cur = conn.cursor()
    cur.fast_executemany = True
    # Read file
    print(f"Reading '{local_fn}'...")
    with open(local_fn, "r", encoding = encoding) as f:
        reader = csv.reader(f)
        src_cols = next(reader) + ["task_name"]
        query = make_insert_query(src_cols, table_name, schema, database, strict_mode)
        if if_exists == "replace":
            cur.execute(f"SELECT COUNT(*) FROM [{schema}].[{table_name}]")
            old_row_count = cur.fetchone()[0]
            if old_row_count:
                print(f"\033[1;33mTruncating {old_row_count} existing rows from [{schema}].[{table_name}]...\033[0m")
                cur.execute(f"TRUNCATE TABLE [{schema}].[{table_name}]")
        elif if_exists != "append":
            raise Exception("if_exists must be 'replace' or 'append'!")
        print(f"Loading data into [{schema}].[{table_name}]...")
        row_count = 0
        start = datetime.now()
        while True:
            params = []
            for row in reader:
                row = [None if r == '' else r for r in row]
                params.append(row + [task_name])
                if len(params) >= batch_size: break
            if params:
                try:
                    cur.executemany(query, params)
                except KeyboardInterrupt:
                    print("Aborted.")
                    sys.exit()
                except:
                    print("\033[1;31mLoad failed. Aborting and trying to find the problem...\033[0m")
                    cur.rollback()
                    bad_row = find_bad_row(query, params, conn)
                    bad_col = find_bad_columns(bad_row, src_cols, table_name, schema, conn)
                    raise
                row_count += len(params)
                if not row_count % 50000:
                    print(f"{row_count} rows loaded in {datetime.now() - start}s...")
                    start = datetime.now()
            else:
                cur.commit()
                print(f"{row_count} rows loaded in {datetime.now() - start}s.")
                return row_count

# Return the first bad row that's causing a failure in in a executemany operations
# Will test params in [steps] steps:
# i.e. If there are 50000 rows in params, it'll test in 100 x 500 row batches
# until it finds a bad batch, then it'll test that batch in 100 x 5 row batches
# until it finds a bad batch, then it'll test that batch in 5 x 1 rows until it
# find the bad row.
def find_bad_row(query, params, conn, steps = 100, is_orig = True):
    if is_orig: print("Looking for bad row in batch...")
    cur = conn.cursor()
    cur.fast_executemany = True
    batch_size = math.ceil(len(params) / steps)
    for i in range(0, len(params), batch_size):
        batch = params[i:i + batch_size]
        try:
            cur.executemany(query, batch)
        except:
            cur.rollback()
            if batch_size == 1:
                print("\033[1;31mBad row found:\033[0m", batch[0])
                return batch[0]
            else:
                return find_bad_row(query, batch, conn, steps, is_orig = False)
    else:
        cur.rollback()
        print("All batches successfully loaded. No bad rows found??")

# Return the columns that are causing problems
# Will attempt to load the same row with different combinations of omitted columns
# i.e. Try without column [A], then without column [B] etc, then without [A, B],
# [A, C], etc, up to all combinations of length [max_omit].
def find_bad_columns(bad_row, src_cols, table_name, schema, conn, max_omit = 3, show_errors = False):
    print("Testing different combination of columns...")
    cur = conn.cursor()
    cur.fast_executemany = True
    bad_row = dict(zip(src_cols, bad_row))
    errors = []
    max_omit = min(max_omit, len(src_cols))
    for l in range(max_omit):
        for omit_cols in itertools.combinations(src_cols, l):
            row = { k:v for k,v in bad_row.items() if k not in omit_cols }
            cols_str = ','.join([f"[{c}]" for c in row.keys()])
            query = (f"INSERT INTO [{schema}].[{table_name}]({cols_str}) "
                     f"VALUES ({','.join(['?'] * len(row))})")
            params = [list(row.values())]
            try:
                cur.executemany(query, params) # DO NOT USE execute(), somethings fail on executemany() but succeed on execute()
                cur.rollback()
                for c in omit_cols:
                    print(f"\033[1;31mRow will load without '{c}': '{bad_row[c]}'\033[0m")
                return omit_cols
            except Exception as e:
                if show_errors: print(f"{list(omit_cols)}: {e}")
                continue # This combination of column removals doesn't work, try the next one
    else:
        cur.rollback()
        print(bad_row)
        print("\033[1;31mNo combination of column removals worked, sorry.\033[0m")


#====================#
#   Authentication   #
#====================#
# Get connection token
# https://github.com/AzureAD/azure-activedirectory-library-for-python/wiki/Connect-to-Azure-SQL-Database
# https://docs.sqlalchemy.org/en/14/dialects/mssql.html#connecting-to-databases-with-access-tokens
def get_conn_token():
    creds = AzureCliCredential() # Use default credentials - use `az cli login` to set this up
    raw_token = creds.get_token("https://database.windows.net/").token.encode("utf-16-le")
    token_struct = struct.pack(f"<I{len(raw_token)}s", len(raw_token), raw_token)
    return { 1256: token_struct } # Connection option for access tokens, as defined in msodbcsql.h


#==================#
#   pyodbc-based   #
#==================#
def pyodbc_conn(database):
    conn = pyodbc.connect(f"{DB_CONN};Database={database};", attrs_before = get_conn_token())
    return conn

def run_query(query, database, mode, verbose = False):
    if verbose: print(f"Running query:", query)
    start = datetime.now()
    conn = pyodbc_conn(database)
    cur = conn.cursor()
    cur.execute(query)
    if mode == "read": pass
    elif mode == "write": cur.commit()
    elif mode == "test": cur.rollback()
    else: raise Exception("Mode must be 'read', 'write', or 'test'!")
    if verbose: print(f"Query completed in {datetime.now() - start}s.")
    return cur

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
    conn_url = URL.create("mssql+pyodbc", query = { "odbc_connect": f"{DB_CONN};Database={database};" })
    engine = create_engine(conn_url, fast_executemany = fast_executemany)
    @event.listens_for(engine, "do_connect")
    def provide_token(dialect, conn_rec, cargs, cparams):
        cargs[0] = cargs[0].replace(";Trusted_Connection=Yes", "") # remove the "Trusted_Connection" parameter that SQLAlchemy adds
        cparams["attrs_before"] = get_conn_token() # Add access token
    return engine

# Load a CSV into SQL with SQLAlchemy
# [DEPRECATED] Has a memory leak issue when used with VARCHAR(max) columns, will cause problems for large files
# But it can be used to create tables on the fly, for when you're too lazy to make a table
def sqlalchemy_loader(local_fn, task, encoding = "utf-8", strict_mode = True, if_exists = "append"):
    task_name = task.task_name
    table_name = task.table_name
    schema = task.schema
    database = task.database
    df = pd.read_csv(local_fn, dtype = str, encoding = encoding)
    if df.memory_usage().sum() > 50000000:
        print("\033[1;33mCAUTION! sqlalchemy_loader() might have trouble handling large files, consider using sql_loader().\033[0m")
    print(f"Loading {len(df)} rows from dataset '{task_name}'...")
    df["task_name"] = task_name
    check_columns(df.columns, table_name, schema, database, strict_mode)
    engine = sqlalchemy_engine(database)
    df.to_sql(table_name, engine, schema, if_exists, index = False, chunksize = 1000)
    return len(df)


#=============#
#   Columns   #
#=============#
def get_columns(table, schema, database):
    query = f"SELECT TOP(1) * FROM [{schema}].[{table}]"
    cur = run_query(query, database, mode = "read")
    return [d[0] for d in cur.description]

def check_columns(src_cols, table, schema, database, strict_mode = True):
    return make_insert_query(src_cols, table, schema, database, strict_mode = True) == None

def make_insert_query(src_cols, table, schema, database, strict_mode = True):
    tbl_cols = get_columns(table, schema, database)
    src_cols = list(src_cols)
    missing_cols = [c for c in tbl_cols if c not in src_cols]
    extra_cols = [c for c in src_cols if c not in tbl_cols]
    usable_cols = [c for c in src_cols if c in tbl_cols]
    # Exact match, no need to name columns
    if [c.lower() for c in src_cols] == [c.lower() for c in tbl_cols]:
        return (f"INSERT INTO [{schema}].[{table}] "
                f"VALUES ({','.join(['?'] * len(usable_cols))})")
    if missing_cols or extra_cols:
        print(f"\033[1;33mExpected columns (from table): {tbl_cols}\033[0m")
        print(f"\033[1;33mActual columns (from data): {src_cols}\033[0m")
        if strict_mode: raise Exception(f"Expected columns are missing or unexpected columns are present!")
    # Otherwise name columns - remember you can have identical columns in the wrong order
    cols_str = ','.join([f"[{c}]" for c in usable_cols])
    return (f"INSERT INTO [{schema}].[{table}]({cols_str}) "
            f"VALUES ({','.join(['?'] * len(usable_cols))})")
