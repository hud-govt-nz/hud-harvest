# Experiments with different ways to bulk insert (part 1)
I went mad and decided to test out every way of bulk inserting data from CSV to databases in R/Python/SQL. Go with `sqlalchemy` since it is the fastest and most robust.

R had a lot of issues with special characters being parsed (e.g. "\n") and making the field sizes bigger than expected, which then caused truncation errors.

Keep `pyodbc` methods around for specific fiddly things, but otherwise use `sqlalchemy` for a good time. (UPDATE: This is untrue for large files. See the INSERT_tests_big.md.)


## R
#### dbWriteTable/dbAppendTable
```R
library(tidyverse)
library(DBI)
grabbed <-
  read_csv("temp/insideairbnb_20220808.csv") %>%
  head(1000)

conn <- dbConnect(odbc::odbc(),
            driver = "{ODBC Driver 18 for SQL Server}",
            server = "property.database.windows.net",
            database = "sqldb-huddevelopment-dev",
            uid = "PropertyUser",
            pwd = "BananaSkin123",
            timeout = 10)

ptm <- proc.time()
dbWriteTable(conn, "scraped.insideairbnb_temp", grabbed, append = TRUE)
print(proc.time() - ptm)

ptm <- proc.time()
dbAppendTable(conn, "scraped.insideairbnb_temp", grabbed) # Same things
print(proc.time() - ptm)
```
**NOPE**
Keeps complaining about `String data, right truncation`, even when all columns are typed to `TEXT`. Could manually fix this in theory but this is far too fiddly to be practical.

#### bcputility
```R
library(tidyverse)
library(bcputility)
grabbed <-
  read_csv("temp/insideairbnb_20220808.csv") %>%
  head(1000)

ptm <- proc.time()
bcpImport(grabbed,
          table = "scraped.insideairbnb_temp",
          driver = "{ODBC Driver 18 for SQL Server}",
          server = "property.database.windows.net",
          database = "sqldb-huddevelopment-dev",
          username = "PropertyUser",
          password = "BananaSkin123",
          trustedconnection = FALSE,
          batchsize = 1000)
print(proc.time() - ptm)
```
**MAYBE, BUT REQUIRES SECOND STAGE**
Cannot cast types, so cannot be used on real table. :-( If you do it on a all-TEXT table it's 60 seconds per 1000 rows, but then you'll need to squish the all-TEXT table back into the real table.


## Python
#### fast_executemany
```py
import pandas as pd
import numpy as np
from datetime import datetime
from common.sqltools import hud_db_connect

df = pd.read_csv("temp/insideairbnb_20220808.csv")
df = df.fillna(np.nan).replace([np.nan], [None])
df["task_name"] = "insideairbnb_test"

cols = df.columns
query = f"INSERT INTO scraped.insideairbnb_raw ({','.join(cols)}) VALUES({','.join(['?'] * len(cols))})"
start = datetime.now()
conn = hud_db_connect("sqldb-huddevelopment-dev")
cur = conn.cursor()
# cur.fast_executemany = True
cur.executemany(query, [r.to_list() for i,r in df.iloc[:1000, :].iterrows()])
print(datetime.now() - start)
```
**MAYBE? SLOWER, BUT MIGHT END UP FASTER IF WE ACCOUNT FOR SECOND STAGE?**
Can't use `fast_executemany`, as that is incompatible with `TEXT` datatypes. Without it, it's 4 minutes per 1000 rows.

#### execute
```py
import pandas as pd
import numpy as np
from datetime import datetime
from common.sqltools import hud_db_connect

df = pd.read_csv("temp/insideairbnb_20220808.csv")
df = df.fillna(np.nan).replace([np.nan], [None])
df["task_name"] = "insideairbnb_test"

cols = df.columns
query = f"INSERT INTO scraped.insideairbnb_raw ({','.join(cols)}) VALUES({','.join(['?'] * len(cols))})"
conn = hud_db_connect("sqldb-huddevelopment-dev")
cur = conn.cursor()
start = datetime.now()
for i,r in df.iloc[:1000, :].iterrows():
  a = cur.execute(query, r.to_list())
else:
  print(datetime.now() - start)
```
**TOO SLOW, FOR DEBUGGING ONLY**
33 minutes per 1000 rows. Purely for debugging since the bulk processes give terrible error messages.

#### bcpy
```py
import bcpy
import pandas as pd
import numpy as np
from datetime import datetime
from common.sqltools import hud_db_connect

df = pd.read_csv("temp/insideairbnb_20220808.csv")
df = df.iloc[:1000]
df = df.fillna(np.nan).replace([np.nan], [None])
df["task_name"] = "insideairbnb_test"

sql_config = {
    "driver": "ODBC Driver 18 for SQL Server",
    "server": "property.database.windows.net",
    "database": "sqldb-huddevelopment-dev",
    "username": "PropertyUser",
    "password": "BananaSkin123"
}
start = datetime.now()
sql_table = bcpy.SqlTable(sql_config, schema = "scraped", table = "insideairbnb_temp")
bdf = bcpy.DataFrame(df)
bdf.to_sql(sql_table)
print(datetime.now() - start)
```
**EUGH, ABANDONWARE, DO NOT USE**

#### sqlalchemy
```py
import pandas as pd
import numpy as np
from common.sqltools import hud_db_connect, pyodbc
from sqlalchemy.engine import URL, create_engine
from datetime import datetime

df = pd.read_csv("temp/insideairbnb_20220808.csv")
df = df.iloc[:1000]
df = df.fillna(np.nan).replace([np.nan], [None])
df["task_name"] = "insideairbnb_test"

connection_string = """
  Driver={ODBC Driver 18 for SQL Server};
  Server=property.database.windows.net;
  Database=sqldb-huddevelopment-dev;
  uid=PropertyUser;
  pwd=BananaSkin123;
"""
connection_url = URL.create("mssql+pyodbc", query={ "odbc_connect": connection_string })
engine = create_engine(connection_url)

start = datetime.now()
df.to_sql("[scraped.insideairbnb_raw", engine, if_exists = "append")
print(datetime.now() - start)
```
**YES!**


## SQL
#### BULK INSERT
**NOPE**
Can't use bulk insert, because that needs to be done server-side.

#### bcp
```
bcp scraped.insideairbnb_temp in temp/insideairbnb_20220808.csv -S property.database.windows.net -d sqldb-huddevelopment-dev -U PropertyUser -P BananaSkin123 -q -c -t ","
```
**SAME AS THE R bcputility**
Works, but cannot handle types properly, so you'll need to do the merge, so might as well do it all in Python/R.
