# Experiments with different ways to bulk insert (part 2)
Turns out, there's a memory leak in df.to_sql() which means very large datasets will eat up all available memory and crash.

```py
import psutil
import pandas as pd
from datetime import datetime
from sqltools import sqlalchemy_engine

process = psutil.Process()

def printmem(msg):
    rss = process.memory_info().rss / 1048576
    vms = process.memory_info().vms / 1048576
    print(f"{msg}: rss: {rss:0.1f} MiB, vms: {vms:0.1f} MiB")

local_fn = "temp/titles_2011-06-05.csv"
db_name = "sqldb-huddevelopment-dev"
schema_name = "linz_historic"
table_name = "titles_changeset"
batch_size = 10000
printmem("Baseline")

# Connect to DB
engine = sqlalchemy_engine(db_name)

print(f"Reading CSV...")
df = pd.read_csv(local_fn, dtype = str)

print(f"Loading to database...")
start = datetime.now()
df.to_sql(table_name, engine, schema_name,
          if_exists = "replace",
          index = False,
          chunksize = 1000)

printmem("Completed")
print(f"Finished in {datetime.now() - start}s.")
```

### What if we ditched SQLAlchemy?
The memory leak is not resolved by removing SQLAlchemy, and it does not occur when the params are generated but not executed. i.e. The batching and params generation alone is not responsible - it's when the params are passed to executemany() that leads to memory leaks (possibly has object references to the individual rows which are never resolved).

```py
import psutil
import pandas as pd
from datetime import datetime
from sqltools import pyodbc_conn

process = psutil.Process()

def printmem(msg):
    rss = process.memory_info().rss / 1048576
    vms = process.memory_info().vms / 1048576
    print(f"{msg}: rss: {rss:0.1f} MiB, vms: {vms:0.1f} MiB")

local_fn = "temp/titles_2011-06-05.csv"
db_name = "sqldb-huddevelopment-dev"
schema_name = "linz_historic"
table_name = "titles_changeset"
batch_size = 10000
printmem("Baseline")

# Connect to DB
conn = pyodbc_conn(db_name)
cur = conn.cursor()
cur.fast_executemany = True

print(f"Reading CSV...")
df = pd.read_csv(local_fn, dtype = str)

print(f"Loading data...")
query = (f"INSERT INTO [{schema_name}].[{table_name}] "
         f"VALUES ({','.join(['?'] * len(df.columns))})")
for i in range(0, len(df), batch_size):
    batch = df.iloc[i:i + batch_size]
    params = [r.to_list() for j,r in batch.iterrows()]
    start = datetime.now()
    cur.executemany(query, params)
    print(f'{datetime.now() - start} seconds')
    printmem(f"After {i + batch_size} rows")

printmem("Completed")
```

### What if we ditched Pandas?
The memory leak is resolved by removing Pandas altogether. The params generation is still there, but whatever it was doing with Pandas does not occur when it is interacting with a simple list.

```py
import psutil, sys, csv
from datetime import datetime
from sqltools import pyodbc_conn

process = psutil.Process()
csv.field_size_limit(sys.maxsize)

def printmem(msg):
    rss = process.memory_info().rss / 1048576
    vms = process.memory_info().vms / 1048576
    print(f"{msg}: rss: {rss:0.1f} MiB, vms: {vms:0.1f} MiB")

local_fn = "temp/titles_2011-06-05.csv"
db_name = "sqldb-huddevelopment-dev"
schema_name = "linz_historic"
table_name = "titles_changeset"
batch_size = 10000
printmem("Baseline")

# Connect to DB
conn = pyodbc_conn(db_name)
cur = conn.cursor()
cur.fast_executemany = True

print(f"Reading CSV...")
with open(local_fn, "r") as f:
    reader = csv.reader(f)
    headers = next(reader)
    query = (f"INSERT INTO [{schema_name}].[{table_name}] "
             f"VALUES ({','.join(['?'] * len(headers))})")
    count = 0
    params = []
    start = datetime.now()
    print(f"Loading data...")
    for row in reader:
        params.append(row)
        count += 1
        if not count % batch_size:
            cur.executemany(query, params)
            print(f"{count} rows loaded in {datetime.now() - start}s...")
            printmem(f"After {count} rows")
            params = []
            start = datetime.now()

printmem("Completed")
```
