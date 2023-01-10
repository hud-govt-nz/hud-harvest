# Tasker
# Task management, database loading and logging layer on top of keeper
import sys
import re
import pandas as pd
import numpy as np
from datetime import datetime
from hudkeep import store, retrieve, local_props, blob_props
from sqltools import run_query, pyodbc_conn

class Task:
    def __init__(self, task_name, schema = "source", database = "property"):
        conn = pyodbc_conn(database)
        self.conn = conn
        self.name = task_name
        self.database = database
        self.schema = schema
        self.get_log()

    # Try to retrieve log information (will return None if it doesn't exist yet)
    def get_log(self):
        cur = self.conn.cursor()
        cur.execute(
            f"SELECT * FROM [{self.schema}].[botlogs] WHERE task_name = ?",
            self.name)
        self.log = cur.fetchone()
        log = self.log or [None] * 11
        self.task_name = log[0]
        self.table_name = log[1]
        self.source_url = log[2]
        self.file_type = log[3]
        self.start_date = log[4]
        self.end_date = log[5]
        self.size = log[6]
        self.hash = log[7]
        self.row_count = log[8]
        self.stored_at = log[9]
        self.loaded_at = log[10]
        return self.log

    # Alter properties in log
    def set_log(self, props):
        cur = self.conn.cursor()
        keys = ",".join([f"{k} = ?" for k in props.keys()])
        cur.execute(
            f"UPDATE [{self.schema}].[botlogs] SET {keys} WHERE task_name = ?",
            *props.values(), self.name)
        cur.commit()
        self.get_log()

    # Create log entry for task - CAN'T DO ANYTHING WITHOUT THIS
    def create(self, table_name, source_url):
        if self.log:
            status(f"{self.name} already exists...", "warning")
        else:
            status(f"Creating '{self.name}'...", "success")
            cur = self.conn.cursor()
            cur.execute(
                f"INSERT INTO [{self.schema}].[botlogs](task_name, table_name, source_url) VALUES(?,?,?)",
                self.name, table_name, source_url)
            cur.commit()
            self.get_log()

    # Store a local file and save metadata to task log
    # Forced will overwrite existing stored file
    def store(self, local_fn, container_url, forced = False):
        if not self.log:
            status(f"'{self.name}' hasn't been created! Run task.create() first.", "error")
            sys.exit()
        l_md5, l_size, l_mtime = local_props(local_fn)
        if not forced and self.hash == l_md5:
            status(f"'{self.name}' is already stored and the stored hash matches the local file.", "warning")
        else:
            ext = re.match(".*\.(\w+)$", local_fn)[1]
            blob_fn = f"{self.table_name}/{self.task_name}.{ext}"
            store(local_fn, blob_fn, container_url, forced)
            status(f"'{self.name}' stored.", "success")
            self.set_log({
                "file_type": ext,
                "hash": l_md5,
                "size": l_size,
                "stored_at": datetime.now()
            })

    # Load task into database and save metadata to task log
    # Loader is a custom function returns a row count
    # Forced will unload before loading
    # Arguments for loader can be included in kwargs
    def load(self, container_url, loader, forced = False, **kwargs):
        if not self.stored_at:
            status(f"'{self.name}' hasn't been stored! Run task.store() first.", "error")
            sys.exit()
        task_name = self.task_name
        table_name = self.table_name
        ext = self.file_type
        blob_fn = f"{table_name}/{task_name}.{ext}"
        b_md5, b_size, b_mtime = blob_props(blob_fn, container_url)
        if b_md5 != self.hash:
            raise Exception("Hash of file stored on the blob doesn't match the logged hash! Something went wrong during .store()?")
        elif not forced and self.loaded_at:
            status(f"'{self.name}' is already loaded and the stored hash matches the local file. Use 'forced = True' if you really want to redo this.", "warning")
        else:
            local_fn = f"temp/{task_name}.{ext}"
            retrieve(local_fn, blob_fn, container_url)
            # if forced:
            #     status(f"Force loading {self.name}...", "warning")
            #     self.unload()
            start = datetime.now()
            row_count = loader(local_fn, self, **kwargs)
            status(f"'{self.name}' loaded ({row_count} rows) in {datetime.now() - start}s.", "success")
            self.set_log({
                "row_count": row_count,
                "loaded_at": datetime.now()
            })

    # # Unload a task from a database (TODO: This is SQL only, needs to be rewritten to be database agnostic)
    # def unload(self):
    #     cur = run_query(
    #         f"DELETE FROM [{self.schema}].[{self.table_name}] WHERE task_name = '{self.task_name}'",
    #         self.database, mode = "write")
    #     status(f"'{self.name}' unloaded ({cur.rowcount} rows) from {self.table_name}.", "warning")
    #     self.set_log({ "loaded_at": None })

# Colourful print very nice
def status(message, status_type):
    if status_type == "success":
        colour = "\033[0;32m"
    elif status_type == "warning":
        colour = "\033[0;33m"
    elif status_type == "error":
        colour = "\033[1;31m"
    print(f"{colour}{message}\033[0m")


#=======================#
#   Table-level tools   #
#=======================#
# Find everything that hasn't been loaded
def get_pending(table_name, schema, database, container_url):
    cur = run_query(
        f"SELECT task_name FROM [{schema}].[botlogs] "
        f"WHERE table_name = '{table_name}' AND loaded_at IS NULL "
        "ORDER BY task_name",
        database, mode = "read")
    return [c[0] for c in cur.fetchall()]
