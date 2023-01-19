# Tasker
# Task management, database loading and logging layer on top of keeper
import sys
import re
import pandas as pd
import numpy as np
from datetime import datetime
from hudkeep import store, retrieve, local_props, blob_props
from sqltools import run_query, pyodbc_conn
from taskmaster import dump_result

class DBLoadTask:
    def __init__(self, task_name, table_name, schema, database = "property", log_table_name = "dbtask_logs"):
        """
        Creates and logs a new DBLoadTask.

        DBLoadTasks are designed to be self-contained and self-logging tasks
        which can be run on its own or as part of a Taskmaster run. Each task
        should be initialised, then

        Parameters
        ----------
        task_name : str
            Unique identifier for this task.
            e.g. "hmu-bot-20230110"
        table_name : str
            Table that this task will load to.
        schema : str
            Schema that this task will load to.
        database : str
            Database that this task will load to.
        log_table_name : str
            Table that this task will log to. This table is expected to be in
            the same schema as the load table.

        Examples
        --------
        t = DBLoadTask(task_name, table_name, schema_name, db_name)
        t.store(local_fn, dst_container, src_url)
        t.load(dst_container, loader = bcp_loader, if_exists = "replace")
        t.dump_result()
        """
        self.conn = pyodbc_conn(database)
        self.task_name = task_name
        self.table_name = table_name
        self.schema = schema
        self.database = database
        self.log_table_name = log_table_name
        # Create log entry for task - CAN'T DO ANYTHING WITHOUT THIS
        self.log = self.get_log()
        if self.log:
            log_msg(f"Task '{task_name}' already exists...", "warning")
        else:
            log_msg(f"Creating new task '{task_name}'...", "success")
            self.new_log()


    #=============#
    #   Actions   #
    #=============#
    def store(self, local_fn, container_url, source_url = "", forced = False):
        """
        Stores a local file in the blob.

        Parameters
        ----------
        local_fn : str
            Local file name.
        container_url : str
            URL for Azure storage container where the file will be stored.
            e.g. "https://dlprojectsdataprod.blob.core.windows.net/bot-outputs"
        source_url : str
            Identifier for where the file came from. This is used to evaluate
            what is a match. Only files with identical table_name, source_url
            and hash are considered matches.
        forced : boolean
            If true, will ignore hash check and store regardless of existing
            files.
        """
        # Don't store if already stored, unless forced to
        store_status = self.log["store_status"]
        if store_status == "success":
            log_msg(f"'{self.task_name}' already been stored!", "warning")
            if forced: log_msg(f"...forcing store() to continue...", "warning")
            else: return # Repeat of the same task - do not update log, do not store
        # Don't store if the file matches the previous stored file with the
        # same table_name/source_url, unless forced to
        last_stored = self.get_last_stored(source_url)
        l_md5, l_size, l_mtime = local_props(local_fn)
        if last_stored and l_md5 == last_stored["hash"]:
            log_msg(f"An identical file was already stored on "
                   f"{last_stored['stored_at']:%Y-%m-%d %H:%M:%S} by "
                   f"'{last_stored['task_name']}'...", "warning")
            if forced: log_msg(f"...forcing store() to continue...", "warning")
            else: return self.set_log({
                "source_url": source_url,
                "store_status": "skipped"
            })
        ext = re.match(".*\.(\w+)$", local_fn)[1]
        blob_fn = f"{self.table_name}/{self.task_name}.{ext}"
        store(local_fn, blob_fn, container_url, forced)
        self.set_log({
            "source_url": source_url,
            "file_type": ext,
            "hash": l_md5,
            "size": l_size,
            "store_status": "success",
            "stored_at": datetime.now()
        })
        log_msg(f"'{self.task_name}' stored.", "success")

    def load(self, container_url, loader, forced = False, **kwargs):
        """
        Loads a data file into the database.

        Mostly relies on store() to check freshness

        Parameters
        ----------
        container_url : str
            URL for Azure storage container where the file will be loaded from.
            e.g. "https://dlprojectsdataprod.blob.core.windows.net/bot-outputs"
        loader : function
            Function which will be used to do the actual loading. Look in the
            sqltools module for examples (e.g. sql_loader(), bcp_loader()).
        forced : boolean
            If true, will ignore hash check and load regardless of existing
            files. This can be dangerous for complex/irreversible loads!
        **kwargs : dict
            Additional arguments are passed to the loader.
        """
        # Don't load if store was not successful
        # If you want to force this, force store()
        store_status = self.log["store_status"]
        if store_status != "success":
            log_msg(f"'{self.task_name}' has a stored_status of '{store_status}'...", "warning")
            if store_status == "skipped":
                log_msg(f"...so load is skipping as well.", "warning")
                return self.set_log({ "load_status": "skipped" })
            else:
                log_msg(f"...have you run task.store() yet?", "error")
                raise Exception("Attempting to load without storing first!")
        # Don't load if already loaded, unless forced to
        load_status = self.log["load_status"]
        if load_status == "success":
            log_msg(f"'{self.task_name}' already been loaded!", "warning")
            if forced: log_msg(f"...forced load() to run anyway...", "warning")
            else: return # Repeat of the same task - do not update log, do not load
        # Don't load if last store hasn't been loaded yet
        source_url = self.log["source_url"]
        last_stored = self.get_last_stored(source_url)
        if last_stored and last_stored["load_status"] != "success":
            log_msg(f"'{source_url}' was stored by '{last_stored['task_name']},' "
                    f"but the load resulted in '{last_stored['load_status']}'!", "warning")
            if forced: log_msg(f"...forced load() to run anyway...", "warning")
            else:
                log_msg(f"Load '{last_store['task_name']}' manually, or run with 'forced = True'.", "warning")
                raise Exception("Attempting to load old tasks unloaded!")
        fn = f"{self.task_name}.{self.log['file_type']}"
        blob_fn = f"{self.table_name}/{fn}"
        local_fn = f"temp/{fn}"
        retrieve(local_fn, blob_fn, container_url)
        start = datetime.now()
        row_count = loader(local_fn, self, **kwargs)
        log_msg(f"'{self.task_name}' loaded ({row_count} rows) in {datetime.now() - start}s.", "success")
        self.set_log({
            "row_count": row_count,
            "load_status": "success",
            "loaded_at": datetime.now()
        })

    # # Unload a task from a database (TODO: This is SQL only, needs to be rewritten to be database agnostic)
    # def unload(self):
    #     cur = run_query(
    #         f"DELETE FROM [{self.schema}].[{self.table_name}] WHERE task_name = '{self.task_name}'",
    #         self.database, mode = "write")
    #     log_msg(f"'{self.task_name}' unloaded ({cur.rowcount} rows) from {self.table_name}.", "warning")
    #     self.set_log({ "loaded_at": None })


    #=========#
    #   Log   #
    #=========#
    def get_log(self):
        cur = self.conn.cursor()
        cur.execute(
            f"SELECT * FROM [{self.schema}].[{self.log_table_name}] WHERE task_name = ?",
            self.task_name)
        row = cur.fetchone()
        if row: return parse_log(row)

    def new_log(self):
        cur = self.conn.cursor()
        cur.execute(
            f"INSERT INTO [{self.schema}].[{self.log_table_name}](task_name, table_name) VALUES(?,?)",
            self.task_name, self.table_name)
        cur.commit()
        self.log = self.get_log()
        return self.log

    def set_log(self, props):
        cur = self.conn.cursor()
        keys = ",".join([f"{k} = ?" for k in props.keys()])
        cur.execute(
            f"UPDATE [{self.schema}].[{self.log_table_name}] SET {keys} WHERE task_name = ?",
            *props.values(), self.task_name)
        cur.commit()
        self.log = self.get_log()
        return self.log

    def get_last_stored(self, source_url):
        cur = self.conn.cursor()
        cur.execute(
            f"SELECT * FROM [{self.schema}].[{self.log_table_name}] "
            f"WHERE source_url=? AND table_name=? AND task_name != ? "
            f"AND store_status = 'success' "
            f"ORDER BY stored_at DESC",
            source_url, self.table_name, self.task_name)
        row = cur.fetchone()
        if row: return parse_log(row)

    # Print results so it can be read by Taskmaster
    def dump_result(self):
        log = self.log.copy()
        status = (log["load_status"], log["load_status"])
        if status == ("success", "success"):
            log["status"] = "success"
        elif status == ("skipped", "skipped"):
            log["status"] = "skipped"
        else:
            log["status"] = "error"
        log["hash"] = log["hash"].hex()
        for k in ["data_start", "data_end", "stored_at", "loaded_at"]:
            if log[k]: log[k] = str(log[k])
        dump_result(log)

def parse_log(row):
    if not row: return None
    return {
        "task_name": row[0],
        "table_name": row[1],
        "source_url": row[2],
        "file_type": row[3],
        "size": row[4],
        "hash": row[5],
        "row_count": row[6],
        "data_start": row[7],
        "data_end": row[8],
        "store_status": row[9],
        "load_status": row[10],
        "stored_at": row[11],
        "loaded_at": row[12]
    }

# Colourful print very nice
def log_msg(message, status_type):
    if status_type == "success":
        colour = "\033[0;32m"
    elif status_type == "warning":
        colour = "\033[0;33m"
    elif status_type == "error":
        colour = "\033[1;31m"
    print(f"{colour}{message}\033[0m")

# Generate a DBLoader task card for sending via Teams
def dbload_card(t):
    STATUS_COLOUR = {
        "success": "good",
        "failed": "attention"
    }
    return {
        "type": "Container",
        "bleed": True,
        "items": [{
            "type": "TextBlock",
            "size": "small",
            "weight": "bolder",
            "text": t["task_name"]
        }, {
            "type": "TextBlock",
            "size": "large",
            "weight": "bolder",
            "spacing": "none",
            "color": STATUS_COLOUR[t["status"]],
            "text": t["status"].upper()
        }, {
            "type":"FactSet",
            "facts":[{
                "title": "Table name",
                "value": t["table_name"]
            }, {
                "title": "Source URL",
                "value": t["source_url"]
            }, {
                "title": "File type",
                "value": t["file_type"]
            }, {
                "title": "Size",
                "value": t["size"]
            }, {
                "title": "Row count",
                "value": t["row_count"]
            }, {
                "title": "Data start",
                "value": t["data_start"]
            }, {
                "title": "Data end",
                "value": t["data_end"]
            }, {
                "title": "Store status",
                "value": t["store_status"]
            }, {
                "title": "Load status",
                "value": t["load_status"]
            }, {
                "title": "Stored at",
                "value": t["stored_at"]
            }, {
                "title": "Loaded at",
                "value": t["loaded_at"]
            }]
        }]
    }


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
