import os, sys, csv, re
import struct
import pyodbc
import math, itertools
import subprocess
import pandas as pd
from datetime import datetime
from sqlalchemy import event
from sqlalchemy.engine import URL, create_engine
from azure.identity import AzureCliCredential

csv.field_size_limit(sys.maxsize)


#=================#
#   Convenience   #
#=================#
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

def insert(row, table_name, schema, database, commit = True):
    conn = pyodbc_conn(database)
    cur = conn.cursor()
    cur.execute(
        f"INSERT INTO [{schema}].[{table_name}]"
        f"({','.join(row.keys())}) "
        f"VALUES({','.join(['?'] * len(row))})",
        *row.values())
    if commit: cur.commit()
    return cur.rowcount

def update(where, set, table_name, schema, database, commit = True):
    conn = pyodbc_conn(database)
    cur = conn.cursor()
    set_str = [f"{k}=?" for k,v in set.items()]
    where_str = [f"{k}=?" for k,v in where.items()]
    cur.execute(
        f"UPDATE [{schema}].[{table_name}] "
        f"SET {','.join(set_str)} "
        f"WHERE {','.join(where_str)}",
        *set.values(), *where.values())
    if commit: cur.commit()
    return cur.rowcount

def delete(where, table_name, schema, database, commit = True):
    conn = pyodbc_conn(database)
    cur = conn.cursor()
    where_str = [f"{k}=?" for k,v in where.items()]
    cur.execute(
        f"DELETE FROM [{schema}].[{table_name}]"
        f"WHERE {','.join(where_str)}",
        *where.values())
    if commit: cur.commit()
    return cur.rowcount

def truncate(table_name, schema, database, commit = True):
    conn = pyodbc_conn(database)
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM [{schema}].[{table_name}]")
    old_row_count = cur.fetchone()[0]
    if old_row_count:
        print(f"\033[1;33mTruncating {old_row_count} existing rows from [{schema}].[{table_name}]...\033[0m")
        cur.execute(f"TRUNCATE TABLE [{schema}].[{table_name}]")
        if commit: cur.commit()

# Generate insert query for bulk inserts
def make_insert_query(src_cols, table_name, schema, database, strict_mode = True):
    tbl_cols = get_columns(table_name, schema, database).keys()
    src_cols = list(src_cols)
    missing_cols = [c for c in tbl_cols if c not in src_cols]
    extra_cols = [c for c in src_cols if c not in tbl_cols]
    usable_cols = [c for c in src_cols if c in tbl_cols]
    # Warn or break on errors
    if missing_cols or extra_cols:
        print(f"\033[1;33mExpected columns (from table): {tbl_cols}\033[0m")
        print(f"\033[1;33mActual columns (from data): {src_cols}\033[0m")
        print(f"\033[1;33mMissing columns: {missing_cols}\033[0m")
        print(f"\033[1;33mUnexpected columns: {extra_cols}\033[0m")
        if strict_mode: raise Exception(f"Expected columns are missing or unexpected columns are present!")
    # Exact match, no need to name columns
    elif [c.lower() for c in src_cols] == [c.lower() for c in tbl_cols]:
        return (f"INSERT INTO [{schema}].[{table_name}] "
                f"VALUES ({','.join(['?'] * len(usable_cols))})")
    # Otherwise name columns - remember you can have identical columns in the wrong order
    else:
        print(f"\033[1;33mUsing named INSERTs (might be slower - ensure columns are identical to avoid this)...\033[0m")
        cols_str = ','.join([f"[{c}]" for c in usable_cols])
        return (f"INSERT INTO [{schema}].[{table_name}]({cols_str}) "
                f"VALUES ({','.join(['?'] * len(usable_cols))})")


#=============#
#   Columns   #
#=============#
def get_columns(table_name, schema, database):
    query = f"""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema}'
        AND TABLE_NAME = '{table_name}'
        ORDER BY ORDINAL_POSITION
    """
    cur = run_query(query, database, "read")
    cols = dict(cur.fetchall())
    return cols

def check_columns(src_cols, table, schema, database, strict_mode = True):
    return make_insert_query(src_cols, table, schema, database, strict_mode = True) == None

def sql_types_to_pandas_types(cols):
    dtype = {}
    parse_dates = []
    for k,v in cols.items():
        if v in ("bigint", "numeric", "bit", "smallint", "decimal", "smallmoney", "int", "tinyint", "money"):
            v = "Int64"
        elif v in ("float", "real"):
            v = "Float64"
        elif v in ("date", "datetimeoffset", "datetime2", "smalldatetime", "datetime", "time"):
            v = "str"
            parse_dates.append(k) # Dates can't be parsed directly, should be defined as strings then passed onto parse_dates
        elif v in ("char", "varchar", "text", "nchar", "nvarchar", "ntext"):
            v = "str"
        else:
            raise Exception(f"I don't know how to parse column type '{v}' for column '{k}'!")
        dtype[k] = v
    return dtype, parse_dates


#================#
#   Connection   #
#================#
# Get connection token
# https://github.com/AzureAD/azure-activedirectory-library-for-python/wiki/Connect-to-Azure-SQL-Database
# https://docs.sqlalchemy.org/en/14/dialects/mssql.html#connecting-to-databases-with-access-tokens
def get_conn_token():
    token = os.getenv("AZURE_TOKEN")
    if not token:
        creds = AzureCliCredential() # Use default credentials - use `az cli login` to set this up
        token = creds.get_token("https://database.windows.net/").token
        os.environ["AZURE_TOKEN"] = token
    raw_token = token.encode("utf-16-le")
    token_struct = struct.pack(f"<I{len(raw_token)}s", len(raw_token), raw_token)
    return { 1256: token_struct } # Connection option for access tokens, as defined in msodbcsql.h

def pyodbc_conn(database):
    DB_CONN = os.getenv("DB_CONN")
    if not DB_CONN: raise Exception("'DB_CONN' not set in '.env'! Read the 'Setting secrets' section in the README.")
    conn = pyodbc.connect(f"{DB_CONN};Database={database};", attrs_before = get_conn_token())
    return conn


#==================#
#   Basic loader   #
#==================#
# Load a CSV into SQL using INSERT + fast_executemany. Has acceptable speeds
# and very good for debugging, but you should switch to bcp_loader for
# production where you just want it to go real fast.
def sql_loader(local_fn, task, if_exists = "append", encoding = "utf-8", strict_mode = True, batch_size = 1000):
    if batch_size > 10000:
        print("\033[1;33mCAUTION! batch_size > 10000 is not recommended!\033[0m")
    task_name = task.task_name
    table_name = task.table_name
    schema = task.schema
    database = task.database
    # Check whether to wipe existing table
    if if_exists == "replace":
        truncate(table_name, schema, database)
    elif if_exists != "append":
        raise Exception("if_exists must be 'replace' or 'append'!")
    # Read file
    print(f"Reading '{local_fn}'...")
    with open(local_fn, "r", encoding = encoding) as f:
        reader = csv.reader(f)
        src_cols = next(reader) + ["task_name"]
        query = make_insert_query(src_cols, table_name, schema, database, strict_mode)
        print(f"Loading data into [{schema}].[{table_name}]...")
        conn = pyodbc_conn(database)
        cur = conn.cursor()
        cur.fast_executemany = True
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


#===============#
#   bcp-based   #
#===============#
# bcp is very fast, but cannot use Active Directory authentication
def bcp_loader(local_fn, task, if_exists = "append", delimiter = "|", encoding = "utf-8", strict_mode = True, batch_size = 100000):
    task_name = task.task_name
    table_name = task.table_name
    schema = task.schema
    database = task.database
    # Check secrets
    DB_CONN = os.getenv("DB_CONN")
    try:
        DB_SERVER = re.search("Server=([^;]*);", DB_CONN, flags = re.IGNORECASE)[1]
        DB_UID = re.search("uid=([^;]*);", DB_CONN, flags = re.IGNORECASE)[1]
        DB_PASS = re.search("pwd=([^;]*);", DB_CONN, flags = re.IGNORECASE)[1]
    except TypeError:
        print("DB_CONN:", DB_CONN)
        raise Exception("bcp_loader() requires server, uid and pwd to be set in the 'DB_CONN' string in '.env'! Read the 'Setting secrets' section in the README.")
    # Check whether to wipe existing table
    if if_exists == "replace":
        truncate(table_name, schema, database)
    elif if_exists != "append":
        raise Exception("if_exists must be 'replace' or 'append'!")
    # Read/clean file
    print(f"Reading '{local_fn}'...")
    temp_fn = f"{local_fn}-bcp_temp.csv"
    start = datetime.now()
    cols = get_columns(table_name, schema, database) # Use datatypes from table
    dtype, parse_dates = sql_types_to_pandas_types(cols)
    df = pd.read_csv(local_fn, dtype = dtype, parse_dates = None) # Don't parse dates, bcp will take care of it
    df.to_csv(temp_fn, sep = delimiter, index = False) # Read and save to use Pandas to clean CSV file
    print(f"File cleaning took {datetime.now() - start}s...")
    # Load
    res = subprocess.run([
        "bcp", f"[{schema}].[{table_name}]",
        "IN", temp_fn,
        "-S", DB_SERVER,
        "-d", database,
        "-U", DB_UID,
        "-P", DB_PASS,
        "-b", str(batch_size),
        "-F", "2",
        "-t", delimiter,
        "-c"
    ])
    os.remove(temp_fn) # Clean up
    try:
        res.check_returncode()
        return len(df)
    except subprocess.CalledProcessError:
        print(f"\033[1;31mbcp failed!\033[0m")
        raise


#======================#
#   sqlalchemy-based   #
#======================#
def sqlalchemy_engine(database, fast_executemany = True):
    DB_CONN = os.getenv("DB_CONN")
    if not DB_CONN: raise Exception("'DB_CONN' not set in '.env'! Read the 'Setting secrets' section in the README.")
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
