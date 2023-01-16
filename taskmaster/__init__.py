#!/bin/python3
import os, sys, argparse, pathlib, logging
import re, json, asyncio, hashlib
import pandas as pd
from datetime import datetime
from sqltools import insert, update, delete

ROOT_PATH = pathlib.Path(__file__).parent.absolute()
STATUSES = {
    "unassigned": "\033[0;35m",
    "success": "\033[0;32m",
    "running": "\033[1;32m",
    "warning": "\033[1;33m",
    "unchanged": "\033[1;30m",
    "failed": "\033[1;31m",
    "default": "\033[1;31m",
    "reset": "\033[0m"
}

# The top-level definition is in jobs.json, which is a list of jobs
# Each job has a name, job-wide parameters, and a list of steps
# Each step is a task, or a list of tasks
# Each task is one R/Python script
class Taskmaster:
    def __init__(self, jobs, run_name = "test_run", scripts_path = "modules", log_db = None):
        self.jobs = jobs
        self.run_name = run_name
        self.run_log = {}
        self.scripts_path = pathlib.Path(scripts_path)
        self.log_db = log_db
        self.log_msgs = []
        self.screen = ""
        self.create_run_log()

    @staticmethod
    def parse_args():
        parser = argparse.ArgumentParser(description = "Runs jobs defined in controller.")
        parser.add_argument("--max-tasks",
                            type = int,
                            default = 8,
                            help = "Number of tasks to run simultaneously (defaults to 8)")
        parser.add_argument("--forced",
                            action = "store_const",
                            const = True,
                            help = "Run even if input hashes are unchanged.")
        parser.add_argument("--only-run",
                            metavar = "O",
                            nargs = "+",
                            help = "Specific scripts to run. All required dependencies will run with it. "
                                   "If blank, all scripts will run (this is usually what you want).")
        return vars(parser.parse_args())

    #=====================#
    #   Task management   #
    #=====================#
    # Runs all tasks until no ready tasks are available
    def run(self, max_tasks = 8, forced = False, only_run = None):
        start = datetime.now()
        run_status = "running"
        tasks = self.tasks = self.list_tasks(self.jobs, only_run)
        self.set_run_log({
            "run_args": str({
                "max_tasks": max_tasks,
                "forced": forced,
                "only_run": only_run
            }),
            "status": run_status,
            "jobs_count": len(self.jobs),
            "tasks_count": len(self.tasks)
        })
        self.print_status() # Print once to allocate lines
        try:
            while True:
                ready = [t for t in tasks if self.is_ready(t, forced)]
                if ready:
                    curr_tasks = [self.run_task(t, forced) for t in ready]
                    operation = gather_with_concurrency(curr_tasks, max_tasks)
                    res = asyncio.run(operation)
                else:
                    # No more tasks ready to run, skip any outstanding tasks
                    for t in tasks:
                        if t["status"] == "unassigned":
                            t["status"] = "skipped"
                    self.print_status()
                    run_status = "finished"
                    break
        except KeyboardInterrupt:
            self.log_msg("Aborting...", "warning")
            run_status = "aborted"
        except AssertionError:
            self.log_msg("Halting due to script error!", "error")
            run_status = "halted"
        except:
            self.log_msg("Crashed!", "error")
            run_status = "crashed"
            raise
        finally:
            self.set_run_log({
                "status": run_status,
                "tasks_succeeded": sum([t["status"] == "success" for t in tasks]),
                "tasks_failed": sum([t["status"] == "failed" for t in tasks]),
                "tasks_skipped": sum([t["status"] == "skipped" for t in tasks]),
                "finished_at": datetime.now()
            })
            self.log_msg(f"\n{run_status.upper()} in {datetime.now() - start}s.", "bold")

    # Break jobs down into interdependent tasks
    def list_tasks(self, jobs, only_run = None):
        tasks = {}
        for job in jobs:
            curr = []
            for step in job["steps"]:
                prev = curr
                curr = []
                if type(step) is not list: step = [step]
                for script in step:
                    task = tasks.get(script) or self.make_task(job, script)
                    tasks[script] = task
                    curr.append(task)
                # Link dependencies
                for t in curr: t["parents"] = get_uniq(t["parents"] + prev)
                for t in prev: t["children"] = get_uniq(t["children"] + curr)
        self.log_msg(f"{len(jobs)} jobs with {len(tasks)} individual tasks loaded.")
        tasks = list(tasks.values())
        if only_run:
            tasks = self.filter_tasks(tasks, only_run)
            self.log_msg(f"Limiting to {len(tasks)} tasks related to {only_run}.")
        return tasks

    # Reduce task list to a selection and its ancestors
    def filter_tasks(self, tasks, only_run):
        selected = []
        for t in tasks:
            if t["script"] in only_run:
                selected.append(t)
                selected += get_ancestors(t)
        selected = get_uniq(selected)
        for t in selected:
            t["children"] = [c for c in t["children"] if c in selected]
        return selected

    # Checks task against last_run
    def is_changed(self, t):
        # k = "input_md5s"
        # lr = t.get("last_run")
        # if not lr: return True # If there is no record of last run, consider changed
        # prev = lr[k]
        # curr = t[k]
        # if not prev or not curr: return True # If there is no input hash, consider changed
        # return str(curr) != str(prev)
        return True

    # Checks whether a task is ready to run
    def is_ready(self, t, forced):
        if t["status"] != "unassigned": return False # Already ran/running
        for d in t.get("parents"):
            if d["status"] != "success": return False # Waiting on dependencies
        if forced or self.is_changed(t):
            return True
        else:
            for c in get_descendents(t) + [t]:
                c["status"] = "unchanged"
                c["start"] = c["end"] = datetime.now()
            self.log_task(t)
            return False


    #================#
    #   Subprocess   #
    #================#
    # Run a single task as a subprocess
    async def run_task(self, t, forced = False):
        self.on_task_ready(t)
        pipe = asyncio.subprocess.PIPE
        proc = await asyncio.create_subprocess_exec(*t["args"], stdout = pipe, stderr = pipe)
        self.start_task(t) # Don't start the task until the subprocess has been created
        try:
            stdout, stderr = [s.decode().strip() for s in await proc.communicate()]
            assert proc.returncode == 0
            self.on_task_complete(t, stdout, stderr)
        except AssertionError:
            t["status"] = "failed"
            t["errors"] = stderr.split("\n")
            self.on_task_fail(t, stdout, stderr, forced)
            if not forced: raise # Ignore fails if forced

        except:
            if proc.returncode is None: # Only terminate if it hasn't finished
                proc.terminate()
                t["status"] = "terminated"
            await proc.wait() # Wait for subprocess to terminate
        finally:
            self.end_task(t)
        return t

    # Verifies a single task and compiles everything it needs to run
    def make_task(self, job, script):
        job_name = job.get("name") or "Unnamed"
        fn = self.scripts_path.joinpath(script)
        if not os.path.isfile(fn):
            raise Exception(f"Script '{fn}' not found (requested by job '{job_name}')!")
        return {
            "script": script,
            "status": "unassigned", # All tasks start out unassigned
            "last_run": self.get_last_run(script), # Fetch last run from log
            "job": job,
            "parents": [],
            "children": []
        }

    # Prepare arguments for subprocesses
    def prep_base_args(self, t):
        name, ext = t["script"].lower().split(".")
        script_fn = str(self.scripts_path.joinpath(t["script"]))
        if ext == "py":
            args = ["pipenv", "run", "python", script_fn]
        elif ext == "r":
            args = ["Rscript", script_fn]
        else:
            raise Exception(f"I don't know how to run files with '.{ext}' extensions!")
        return args


    #=================#
    #   Task events   #
    #=================#
    # REPLACE EVENTS WITH CUSTOM FUNCTIONS
    # Before task has started
    def on_task_ready(self, t):
        t["args"] = self.prep_base_args(t)
        t["args"] += self.prep_extra_args(t)

    # If task returned a code == 0
    def on_task_complete(self, t, stdout, stderr):
        r = read_result(stdout)
        t.update(r) # All outputs are saved to the task

    # If task returned a code != 0
    def on_task_fail(self, t, stdout, stderr, forced):
        if forced: return
        safe_args = [re.sub(r"([\s])", r"\\\1", a) for a in t["args"]]
        self.dump = (
            f"\n\033[1;36m===  stdout  ===\033[0m\n{stdout}" +
            f"\n\033[1;36m===  stderr  ===\033[0m\n{stderr}" +
            f"\n\033[1;36m===  Command  ===\033[0m\n{' '.join(safe_args)}"
        )


    #=============#
    #   Logging   #
    #=============#
    def print_status(self):
        if self.screen:
            for l in self.screen.split("\n"):
                print('\033[1A', end='\x1b[2K')
        self.screen = ""
        if hasattr(self, "tasks"):
            self.screen += draw_tree(self.tasks) + "\n"
        if hasattr(self, "log_msg"):
            self.screen += draw_message_box(self.log_msgs) + "\n"
        if hasattr(self, "dump"):
            self.screen += self.dump
        print(self.screen)

    def log_msg(self, message, level = "info"):
        message = message.strip()
        self.log_msgs.append((datetime.now(), message, level))
        self.print_status()

    def create_run_log(self):
        if not self.log_db: return
        where = { "run_name": self.run_name }
        row_count = delete(where, **self.log_db)
        if row_count:
            self.log_msg(f"Replacing existing log for {self.run_name}...", "warning")
        row = {
            "run_name": self.run_name,
            "status": "started",
            "started_at": datetime.now(),
        }
        self.run_log.update(row)
        insert(row, **self.log_db)

    def set_run_log(self, set):
        if not self.log_db: return
        where = {
            "run_name": self.run_name
        }
        self.run_log.update(set)
        update(where, set, **self.log_db)

    def start_task(self, t):
        t["start"] = datetime.now()
        t["status"] = "running"
        self.log_msg(f"{t['script']} starting...")

    def end_task(self, t):
        t["end"] = datetime.now()
        if t["status"] == "failed":
            self.log_msg(f"{t['script']} failed!", "error")
        else:
            self.log_msg(f"{t['script']} finished with status {t['status']}.")

    def get_last_run(self, s):
        # df = pd.read_csv(f"{ROOT_PATH}/log.csv")
        # df = df.query(f"script == '{s}' and status == 'success'").sort_values("end")
        # if len(df): return dict(df.iloc[-1])
        return None


#===========#
#  Helpers  #
#===========#
def get_uniq(items):
    out = []
    for i in items:
        if i not in out: out.append(i)
    return out

def get_ancestors(t):
    out = [] + t["parents"]
    for p in t["parents"]:
        out += get_ancestors(p)
    return out

def get_descendents(t):
    out = [] + t["children"]
    for c in t["children"]:
        out += get_descendents(c)
    return out

# Use Semaphore to limit the number of concurrent tasks
async def gather_with_concurrency(tasks, max_tasks):
    semaphore = asyncio.Semaphore(max_tasks) # Use Semaphore to limit the number of concurrent tasks
    async def sem_task(task):
        async with semaphore:
            return await task
    res = await asyncio.gather(*(sem_task(task) for task in tasks))
    return res


#===========#
#  Results  #
#===========#
# Wrap JSON dump so we can pluck it out of stdout
def dump_result(payload):
    out = "== RESULT START ==\n"
    out += json.dumps(payload)
    out += "\n== RESULT END =="
    sys.stdout.write(out)

# Extract results from a text block
def read_result(raw):
    exp = r"== RESULT START ==\n(.*)\n== RESULT END =="
    res = re.search(exp, raw)[1]
    out = json.loads(res)
    return out

# Looks for output and hashes it
def hash_output(t):
    urls = t.get("output_urls")
    if not urls: return
    t["output_md5s"] = []
    if type(urls) is str: urls = urls.split(", ")
    for url in urls:
        with open(url, "rb") as f:
            h = hashlib.md5(f.read())
            t["output_md5s"].append(h.hexdigest())


#===============#
#  Report/logs  #
#===============#
def draw_message_box(messages):
    messages = messages[-6:]
    out = "\033[1;30m=======================  Log  =======================\033[0m\n"
    out += "\n" * (6 - len(messages))
    for d,m,l in messages:
        if l == "bold":
            colour = "\033[1m"
        elif l == "error":
            colour = "\033[1;31m"
        elif l == "warning":
            colour = "\033[1;33m"
        else:
            colour = ""
        out += f"{d:%H:%M:%S}: {colour}{m}\033[0m\n"
    out += "\033[1;30m=====================================================\033[0m"
    return out

def draw_tree(tasks):
    top_level = [t for t in tasks if not t["parents"]]
    out = ""
    for t in top_level:
        out += draw_branch(t, is_top = True)
    return out

# Recursive branch print used by draw_tree()
def draw_branch(t, prefix = "", is_top = False, is_last = False):
    script = t["script"]
    status = t["status"]
    colour = STATUSES.get(status) or STATUSES["default"]
    body = f"{script} [{colour}{status}\033[0m]\n"
    if is_top:
        out = f"\n{body}"
        prefix = ""
    elif is_last:
        out = f"{prefix}└── {body}"
        prefix += "    "
    else:
        out = f"{prefix}├── {body}"
        prefix += "│   "
    for c in t["children"]:
        out += draw_branch(c, prefix, is_last = c == t["children"][-1])
    return out
