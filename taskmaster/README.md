# Taskmaster
Task orcestration framework.

* The unit of Taskmaster is a Task, which is a single script which can be run on its own.
* One or more Tasks form a Step
* One or more Steps form a Job
* One or more Jobs form a Run

The example below is a `jobs.json` file, which describes three Jobs, each with one Step, and each of those steps containing one Task. This file is used to create a Run.
```
[{
    "name": "Bonds",
    "steps": [
        "bonds/bonds.py"
    ]
}, {
    "name": "Landlord Contact Service",
    "steps": [
        "bonds/llcontact.py"
    ]
}, {
    "name": "Landlord Property",
    "steps": [
        "bonds/llproperty.py"
    ]
}]
```

### Task
Taskmaster will run tasks as [asynchronous subprocesses](https://docs.python.org/3/library/asyncio-subprocess.html). This means, where possible, many tasks will run in parallel.

Each task must be:
* A script which can be run on its own (R & Python are supported, but in theory it can be anything).
* Print its output in valid JSON between `== RESULT START ==` and `== RESULT END ==` strings. e.g.:
```
== RESULT START ==
{"status": "success", "task_name": "bonds_test", "table_name": "bonds_test", "source_url": "MBIE/Tenancy_Bonds/Bonds.txt", "file_type": "txt", "start_date": "None", "end_date": "None", "size": 790534816, "hash": "99a1fd2c7567e0d268357c72d8da1c24", "row_count": 5339667, "stored_at": "2023-01-10 05:05:43.050000", "loaded_at": "2023-01-17 09:09:14.567000"}
== RESULT END ==
```
* Must have a `status` property its output, and this must be `success` if the Task completed successfully. **Taskmaster will treat any other value as failures.**

### Jobs & steps
Jobs are provide parameters to tasks, and Steps to manage the order in which they run. These are read during events. e.g.:
```
[{
    "name": "Rental Price Index change",
    "subfolder": "Rental Market",
    "priority": 0,
    "frequency": 1,
    "steps": [
        "statsnz-rental_price_index-grab.py",
        "statsnz-rental_price_index-publish.R"
    ]
}, {
    "name": "Housing register change",
    "subfolder": "Rental Market",
    "priority": 1,
    "frequency": 1,
    "steps": [
        "msd-housing_register-grab.py",
        "msd-housing_register-publish.R"
    ]
}, {
    "name": "New rental listings",
    "subfolder": "Rental Market",
    "priority": 1,
    "frequency": 1,
    "steps": [
        "otm-rental_listings-grab.R",
        "otm-rental_listings_started-publish.R"
    ]
}
```

### Events
Tasks have three types of events:
* `on_task_ready`: Triggered when a Task is ready to run (i.e. When it's not waiting for any preceding steps to finish). This is used to prepare the arguments that will be passed to the script when it is run. e.g. We might use it to tell `statsnz-rental_price_index-grab.py` to put its output in the `Rental Market` subfolder.
* `on_task_complete`: Triggered when a Task is completed with a return code of 0. This is used to check the results and save it in the Task, but can also be used for logging and notifications.
* `on_task_fail`: Triggered when a Task is completed with a return code other than 0. This is used for error reporting and notifications.

#### How is data passed between tasks?
Each Task object is aware of the Job that it's a part of (`task["job"]`), and the Tasks that it's dependent on (`task["parents"]` i.e. Tasks in preceding Steps), and the Tasks that depends on it (`task["children"]`). Tasksmaster does all of these dependency calculations on start-up.

During any of the events, you can access the results of these Jobs or related Tasks. e.g.:
```
def on_task_ready(self, t):
    t["args"] = self.prep_base_args(t) # Base arguments for running the Task
    for p in t["parents"]: # For each parent Task
        t["args"].append(p["output_url"]) # # Add the parent's output to this task's run-time arguments
```
