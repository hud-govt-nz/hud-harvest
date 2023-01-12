#!/bin/python3
import os, sys, argparse, pathlib, logging
import re, json, time, asyncio, hashlib
from datetime import datetime
import pandas as pd

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

# Make log buffer
logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S",
                    level = logging.INFO)


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


#=============#
#  Run tasks  #
#=============#
# Use Semaphore to limit the number of concurrent tasks
async def gather_with_concurrency(tasks, max_tasks):
    semaphore = asyncio.Semaphore(max_tasks) # Use Semaphore to limit the number of concurrent tasks
    async def sem_task(task):
        async with semaphore:
            return await task
    res = await asyncio.gather(*(sem_task(task) for task in tasks))
    return res

# Checks task against last_run
def is_changed(t):
    k = "input_md5s"
    lr = t.get("last_run")
    if not lr: return True # If there is no record of last run, consider changed
    prev = lr[k]
    curr = t[k]
    if not prev or not curr: return True # If there is no input hash, consider changed
    return str(curr) != str(prev)

# Checks whether a task is ready to run
def is_ready(t, forced):
    if t["status"] != "unassigned": return False # Already ran/running
    for d in t.get("parents"):
        if d["status"] != "success": return False # Waiting on dependencies
    if forced or is_changed(t):
        return True
    else:
        for c in get_descendents(t) + [t]:
            c["status"] = "unchanged"
            c["start"] = c["end"] = datetime.now()
        write_log(t)
        return False


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

def dump_task(args, stdout, stderr):
    print_divider("stdout")
    logging.error(stdout)
    print_divider("stderr")
    logging.error(stderr)
    print_divider("Command")
    safe_args = [re.sub(r"([\s])", r"\\\1", a) for a in args]
    print(" ".join(safe_args))

def print_divider(name, colour = "\033[1;36m"):
    print(f"\n{colour}===  {name}  ===\033[0m")


#===============#
#  Report/logs  #
#===============#
def print_tree(tasks, clear = True):
    top_level = [t for t in tasks if not t["parents"]]
    if clear:
        for i in range(0, len(tasks) + len(top_level)):
            print('\033[1A', end='\x1b[2K')
    for t in top_level:
        print_branch(t, is_top = True)

# Recursive branch print used by print_tree()
def print_branch(t, prefix = "", is_top = False, is_last = False):
    script = t["script"]
    status = t["status"]
    colour = STATUSES.get(status) or STATUSES["default"]
    body = f"{script} [{colour}{status}\033[0m]"
    if is_top:
        print(f"\n{body}")
        prefix = ""
    elif is_last:
        print(f"{prefix}└── {body}")
        prefix += "    "
    else:
        print(f"{prefix}├── {body}")
        prefix += "│   "
    for c in t["children"]:
        print_branch(c, prefix, is_last = c == t["children"][-1])

def write_log(t):
    # df = pd.read_csv(f"{ROOT_PATH}/log.csv")
    # df = pd.concat([df, pd.DataFrame([t])])
    # df[LOG_COLS].to_csv(LOG_FN, index=False)
    return None

def get_last_run(s):
    # df = pd.read_csv(f"{ROOT_PATH}/log.csv")
    # df = df.query(f"script == '{s}' and status == 'success'").sort_values("end")
    # if len(df): return dict(df.iloc[-1])
    return None

#=======#
#  Run  #
#=======#
# The top-level definition is in jobs.json, which is a list of jobs
# Each job has a name, job-wide parameters, and a list of steps
# Each step is a task, or a list of tasks
# Each task is one R/Python script
class Taskmaster:
    def __init__(self, jobs, run_name = "test_run", script_path = "modules"):
        self.jobs = jobs
        self.run_name = run_name
        self.script_path = pathlib.Path(script_path)

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
        parser.add_argument("--debug",
                            action = "store_const",
                            const = True,
                            help = "Halt and catch fire on errors.")
        parser.add_argument("--only-run",
                            metavar = "O",
                            nargs = "+",
                            help = "Specific scripts to run. All required dependencies will run with it. "
                                   "If blank, all scripts will run (this is usually what you want).")
        return vars(parser.parse_args())

    def log(self, message, level = "info"):
        logging.info(message)

    # Verifies a single task and compiles everything it needs to run
    def make_task(self, job, script):
        name = job.get("name") or "Unnamed"
        fn = self.script_path.joinpath(script)
        if not os.path.isfile(fn):
            raise Exception(f"Script '{fn}' not found (requested by job '{name}')!")
        return {
            "script": script,
            "job": job,
            "status": "unassigned", # All tasks start out unassigned
            "last_run": get_last_run(script), # Fetch last run from log
            "parents": [],
            "children": []
        }

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
        self.log(f"{len(jobs)} jobs with {len(tasks)} individual tasks loaded.")
        tasks = list(tasks.values())
        if only_run:
            tasks = self.filter_tasks(tasks, only_run)
            self.log(f"Limiting to {len(tasks)} tasks related to {only_run}.")
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

    # Prepare arguments for subprocesses
    def prep_args(self, t):
        name, ext = t["script"].lower().split(".")
        script_fn = str(self.script_path.joinpath(t["script"]))
        if ext == "py":
            args = ["pipenv", "run", "python", script_fn]
        elif ext == "r":
            args = ["Rscript", script_fn]
        else:
            raise Exception(f"I don't know how to run files with '.{ext}' extensions!")
        args += self.prep_extra_args(t)
        return args

    # Replace this with a custom function if you want to pass extra arguments
    def prep_extra_args(self, t):
        return []

    # Run a single task
    async def run_task(self, t, debug = False):
        # Initialise task
        t["start"] = datetime.now()
        t["status"] = "running"
        # Run subprocess
        pipe = asyncio.subprocess.PIPE
        args = self.prep_args(t)
        proc = await asyncio.create_subprocess_exec(*args, stdout = pipe, stderr = pipe)
        stdout, stderr = [s.decode().strip() for s in await proc.communicate()]
        # Process status/output
        try:
            if proc.returncode != 0:
                raise Exception(f"{t['script']} failed!")
            r = read_result(stdout)
            t.update(r) # All outputs are saved to the task
            print_tree(self.tasks)
        except:
            t["status"] = "failed"
            t["errors"] = stderr.split("\n")
            print_tree(self.tasks)
            if debug:
                dump_task(args, stdout, stderr)
                print_divider("Traceback")
                raise
        # Checkout task
        t["end"] = datetime.now()
        write_log(t)
        return t

    # Runs all tasks until no ready tasks are available
    def run(self, max_tasks = 8, forced = False, debug = False, only_run = None):
        start = datetime.now()
        tasks = self.tasks = self.list_tasks(self.jobs, only_run)
        print_tree(tasks, clear = False) # Print once to allocate lines
        try:
            while True:
                ready = [t for t in tasks if is_ready(t, forced)]
                if ready:
                    curr_tasks = [self.run_task(t, debug) for t in ready]
                    print_tree(tasks) # Update once all the ready tasks have been assigned
                    operation = gather_with_concurrency(curr_tasks, max_tasks)
                    res = asyncio.run(operation)
                else:
                    # No more tasks ready to run, skip any outstanding tasks
                    for t in tasks:
                        if t["status"] == "unassigned":
                            t["status"] = "skipped"
                    print_tree(tasks)
                    self.log(f"\nFinished in {datetime.now() - start}s.")
                    break
        except KeyboardInterrupt:
            self.log("Aborted.")
