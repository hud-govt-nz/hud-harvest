#!/bin/python3
import os, sys, argparse, pathlib, logging
import re, json, time, asyncio, hashlib
from datetime import datetime
import pandas as pd

ROOT_PATH = pathlib.Path(__file__).parent.absolute()
LOG_COLS = ["script", "status", "errors", "start", "end", "input_urls", "input_md5s", "output_urls", "output_md5s"]
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

def print_divider(name, colour = "\033[1;36m"):
    print(f"\n{colour}===  {name}  ===\033[0m")


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
def is_changed(t, k):
    lr = t.get("last_run")
    if not lr: return True # If there is no record of last run, consider changed
    prev = lr[k]
    curr = t[k]
    if not prev or not curr: return True # If there is no input hash, consider changed
    return str(curr) != str(prev)

# Checks whether a task is ready to run
def is_ready(t, forced):
    if t["status"] != "unassigned": return False # Already ran/running
    deps = t.get("parents") or []
    if any([d["status"] != "success" for d in deps]): return False # Waiting on dependencies
    for i,o in [("input_urls", "output_urls"), ("input_md5s", "output_md5s")]:
        t[i] = []
        for d in t.get("parents") or []:
            if type(d[o]) is list: t[i] += d[o]
            else: t[i].append(d[o])
    # Check input hashes
    if forced or is_changed(t, "input_md5s"):
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


#===============#
#  Report/logs  #
#===============#
# Recursive branch print used by update_state()
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
class Taskmaster:
    def __init__(self, jobs, proj_name = "bot", root_path = ROOT_PATH):
        self.name = proj_name
        self.path = root_path
        self.script_path = pathlib.Path(f"{self.path}/scripts")
        self.output_path = pathlib.Path(f"{self.path}/outputs")
        self.jobs = jobs
        self.tasks = self.calc_tasks(jobs)

    def update_state(self, clear = True):
        tasks = self.tasks
        top_level = [t for t in tasks if not t["parents"]]
        if clear:
            for i in range(0, len(tasks) + len(top_level)):
                print('\033[1A', end='\x1b[2K')
        for t in top_level:
            print_branch(t, is_top = True)

    # Create and verify a single task
    def make_task(self, job, script):
        name = job.get("name") or "Unnamed"
        fn = self.script_path.joinpath(script)
        if not os.path.isfile(fn):
            raise Exception(f"Script '{script}' not found (requested by job '{name}')!")
        return {
            "name": name,
            "script": script,
            "subfolder": job.get("subfolder"),
            "status": "unassigned", # All tasks start out unassigned
            "last_run": get_last_run(script), # Fetch last run from log
            "parents": [],
            "children": []
        }

    # Break jobs down into interdependent tasks
    def calc_tasks(self, jobs):
        tasks = {}
        for j in jobs:
            curr = []
            for step in j["tasks"]:
                prev = curr
                curr = []
                if type(step) is not list: step = [step]
                for s in step:
                    tasks[s] = tasks.get(s) or self.make_task(j, s)
                    curr.append(tasks[s])
                # Link dependencies
                for t in curr: t["parents"] = prev
                for t in prev: t["children"] = get_uniq(t["children"] + curr)
        logging.info(f"{len(jobs)} jobs with {len(tasks)} individual tasks loaded.")
        return list(tasks.values())

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
    def prep_args(self, t, batch_name):
        name, ext = t["script"].lower().split(".")
        if ext == "py":
            args = "pipenv run python3".split(" ")
        elif ext == "r":
            args = "Rscript".split(" ")
        else:
            raise Exception(f"I don't know how to run files with '.{ext}' extensions!")
        script = self.script_path
        output = self.output_path.joinpath(batch_name)
        if t.get("subfolder"): output = output.joinpath(t["subfolder"])
        args += [
            str(script.joinpath(t["script"])),
            str(output.joinpath(name))
        ]
        args += t.get("input_urls") or []
        return args

    # Run a single task
    async def run_task(self, t, batch_name, debug = False):
        # Initialise task
        t["start"] = datetime.now()
        t["status"] = "running"
        # Run subprocess
        pipe = asyncio.subprocess.PIPE
        args = self.prep_args(t, batch_name)
        proc = await asyncio.create_subprocess_exec(*args, stdout = pipe, stderr = pipe)
        stdout, stderr = [s.decode().strip() for s in await proc.communicate()]
        # Process status/output
        try:
            if proc.returncode != 0:
                raise Exception(f"{t['script']} failed!")
            r = read_result(stdout)
            t.update(r)
            hash_output(t)
        except:
            t["status"] = "failed"
            t["errors"] = stderr.split("\n")
            self.update_state()
            if debug:
                dump_task(args, stdout, stderr)
                print_divider("Traceback")
                raise
        # Checkout task
        t["end"] = datetime.now()
        self.update_state()
        write_log(t)
        return t

    # Runs all tasks until no ready tasks are available
    def run(self, max_tasks = 8, forced = False, debug = False, only_run = None):
        batch_name = f"{self.name}-{datetime.now():%Y%m%d}"
        if only_run:
            self.tasks = self.filter_tasks(self.tasks, only_run)
            logging.info(f"Limiting to {len(self.tasks)} tasks related to {only_run}.")
        # Create root output folders
        tasks = self.tasks
        batch_path = self.output_path.joinpath(batch_name)
        for p in [self.output_path, batch_path]:
            if not os.path.exists(p): os.mkdir(p)
        # Create subfolders
        for t in tasks:
            if t.get("subfolder"):
                p = batch_path.joinpath(t["subfolder"])
                if not os.path.exists(p): os.mkdir(p)
        try:
            self.update_state(clear = False) # Print once to allocate lines
            while True:
                ready = [t for t in tasks if is_ready(t, forced)]
                if ready:
                    curr_tasks = [self.run_task(t, batch_name, debug) for t in ready]
                    self.update_state() # Update once all the ready tasks have been assigned
                    operation = gather_with_concurrency(curr_tasks, max_tasks)
                    res = asyncio.run(operation)
                else:
                    break
            for t in tasks:
                if t["status"] == "unassigned":
                    t["status"] = "incomplete"
            self.update_state()
            print("\nDone.")
        except KeyboardInterrupt:
            logging.info("Aborted.")
