#!/bin/python3
import os, sys, argparse, pathlib, logging
import re, json, asyncio, hashlib
import pandas as pd
from datetime import datetime
from sqltools import insert, update, delete
from chatter import send_card

ROOT_PATH = pathlib.Path(__file__).parent.absolute()
STATUSES = {
    "unassigned": "\033[0;35m",
    "success": "\033[0;32m",
    "running": "\033[1;32m",
    "warning": "\033[1;33m",
    "unchanged": "\033[1;30m",
    "skipped": "\033[1;30m",
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
        self.auto = True # Start in auto mode
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
        parser.add_argument("--auto",
                            action = "store_const",
                            const = True,
                            help = "Run unsupervised and send results via Teams.")
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
    def run(self, auto = False, forced = False, only_run = None, max_tasks = 8):
        start = datetime.now()
        run_status = "running"
        self.auto = auto
        self.forced = forced
        self.tasks = tasks = self.list_tasks(self.jobs, only_run)
        self.set_run_log({
            "run_args": str({
                "auto": forced,
                "forced": forced,
                "only_run": only_run,
                "max_tasks": max_tasks
            }),
            "status": run_status,
            "jobs_count": len(self.jobs),
            "tasks_count": len(tasks)
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
            self.on_run_complete()

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
        t["start"] = datetime.now()
        t["status"] = "running"
        self.log_msg(f"{t['script']} starting...") # Don't start the task until the subprocess has been created
        try:
            stdout, stderr = [s.decode().strip() for s in await proc.communicate()]
            t["end"] = datetime.now()
            assert proc.returncode == 0
            self.on_task_complete(t, stdout, stderr)
            self.log_msg(f"{t['script']} finished with status '{t['status']}'.")
        except AssertionError:
            self.on_task_fail(t, stdout, stderr, forced)
            self.log_msg(f"{t['script']} failed!", "error")
            if not forced: raise # Ignore fails if forced
        finally:
            if proc.returncode is None: # Only terminate if it hasn't finished
                proc.terminate()
                await proc.wait() # Wait for subprocess to terminate
                t["status"] = "terminated"
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

    # If task returned a code == 0
    def on_task_complete(self, t, stdout, stderr):
        r = read_result(stdout)
        t.update(r) # All outputs are saved to the task

    # If task returned a code != 0
    def on_task_fail(self, t, stdout, stderr, forced):
        t["status"] = "failed"
        if forced: return
        self.dump = (t, stdout, stderr)

    # When the entire run is finished
    def on_run_complete(self):
        if not self.auto: return # Only report when running in auto
        print("Sending run report...")
        body = [simple_run_card(**self.run_log)] # Default run notification
        if hasattr(self, "dump"): body.append(dump_card(*self.dump))
        body.append({
            "type":"TextBlock",
            "text":"Ping <at>Keith Ng</at>"
        })
        entities = [{
            "type": "mention",
            "text": "<at>Keith Ng</at>",
            "mentioned": {
                "id": "keith.ng@hud.govt.nz",
                "name": "Keith Ng"
            }
        }]
        send_card(body, entities)


    #=============#
    #   Logging   #
    #=============#
    def print_status(self, logs_size = 6):
        if self.auto: return # No in-place screen in auto mode
        if self.screen:
            for l in self.screen.split("\n"):
                print('\033[1A', end='\x1b[2K')
        self.screen = ""
        if hasattr(self, "tasks"):
            self.screen += draw_tree(self.tasks) + "\n"
        if hasattr(self, "log_msg"):
            self.screen += draw_message_box(self.log_msgs, logs_size) + "\n"
        if hasattr(self, "dump"):
            self.screen += draw_dump(*self.dump)
        print(self.screen)

    def log_msg(self, message, level = "info"):
        message = message.strip()
        log_msg = (datetime.now(), message, level)
        self.log_msgs.append(log_msg)
        self.print_status()
        if self.auto: print(draw_message(*log_msg)) # Print messages immediate in auto mode

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
    out = "\n== RESULT START ==\n"
    out += json.dumps(payload)
    out += "\n== RESULT END ==\n"
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
def draw_message(msg_datetime, message, level = "info"):
    LEVEL_COLOURS = {
        "bold": "\033[1m",
        "error": "\033[1;31m",
        "warning": "\033[1;33m",
        "info": ""
    }
    return f"{msg_datetime:%H:%M:%S}: {LEVEL_COLOURS[level]}{message}\033[0m"

def draw_message_box(messages, logs_size = 6):
    messages = messages[-logs_size:]
    out = ["\033[1;30m=======================  Log  =======================\033[0m"]
    out += [""] * (logs_size - len(messages))
    out += [draw_message(*m) for m in messages]
    out += ["\033[1;30m=====================================================\033[0m"]
    return "\n".join(out)

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

def draw_dump(t, stdout, stderr):
    safe_args = [re.sub(r"([\s])", r"\\\1", a) for a in t["args"]]
    return (
        f"\n\033[1;36m===  stdout  ===\033[0m\n{stdout or '[stdout is empty]'}" +
        f"\n\033[1;36m===  stderr  ===\033[0m\n{stderr or '[stderr is empty]'}" +
        f"\n\033[1;36m===  Command  ===\033[0m\n{' '.join(safe_args)}"
    )


#===========#
#   Teams   #
#===========#
# Default run-card
def simple_run_card(run_name, run_args, jobs_count, tasks_count, tasks_succeeded, tasks_failed, tasks_skipped, status, started_at, finished_at):
    STATUS_COLOUR = {
        "finished": "good",
        "aborted": "warning",
        "halted": "attention",
        "crashed": "attention"
    }
    return {
        "type": "Container",
        "style": "accent",
        "bleed": True,
        "items": [{
            "type": "TextBlock",
            "size": "small",
            "weight": "bolder",
            "text": run_name
        }, {
            "type": "TextBlock",
            "size": "large",
            "weight": "bolder",
            "spacing": "none",
            "color": STATUS_COLOUR[status],
            "text": status.upper()
        }, {
            "type":"FactSet",
            "facts":[{
                "title": "Jobs:",
                "value": f"{tasks_count} tasks from {jobs_count} jobs"
            }, {
                "title": "Outcome:",
                "value": f"{tasks_succeeded} tasks succeeded, {tasks_failed} failed, {tasks_skipped} skipped"
            }, {
                "title": "Run time:",
                "value": f"{str(finished_at - started_at)[:-4]}"
            }, {
                "title": "Args:",
                "value": run_args
            }]
        }]
    }

# Card for reporting errors
def dump_card(t, stdout, stderr):
    safe_args = [re.sub(r"([\s])", r"\\\1", a) for a in t["args"]]
    return {
        "type": "Container",
        "style": "warning",
        "bleed": True,
        "items": [{
            "type": "TextBlock",
            "size": "small",
            "text": "Error while running:"
        }, {
            "type": "TextBlock",
            "spacing": "small",
            "fontType": "Monospace",
            "weight": "Bolder",
            "wrap": True,
            "text": " ".join(safe_args)
        }, {
            "type": "TextBlock",
            "size": "small",
            "weight": "Bolder",
            "color": "Accent",
            "text":"==  stdout  =="
        }, {
            "type": "TextBlock",
            "size": "small",
            "spacing": "none",
            "fontType": "Monospace",
            "wrap": True,
            "text": re.sub(r"\n", r"\n\n", stdout) or "_stdout is empty_"
        }, {
            "type": "TextBlock",
            "size": "small",
            "weight": "Bolder",
            "color": "Accent",
            "text":"==  stderr  =="
        }, {
            "type": "TextBlock",
            "size": "small",
            "spacing": "none",
            "fontType": "Monospace",
            "wrap": True,
            "text": re.sub(r"\n", r"\n\n", stderr) or "_stderr is empty_"
        }]
    }
