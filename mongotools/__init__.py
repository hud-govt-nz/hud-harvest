import sys
import re
import pymongo
import ijson

#=============#
#   Loaders   #
#=============#
# Load a JSON file into a Mongo database
def mongo_loader(local_fn, task, path = "item", batch_size = 2000, sanitise = False):
    task_name = task.task_name
    table_name = task.table_name
    print(f"Loading JSON from file '{local_fn}'...")
    col = get_collection(table_name)
    if sanitise: local_fn = sanitise_json(local_fn)
    with open(local_fn, "rb") as f:
        entities = ijson.items(f, path, use_float = True)
        r = 0
        w = 0
        while True:
            batch = []
            for d in entities:
                d["_task_name"] = task_name
                batch.append(d)
                if len(batch) >= batch_size: break
            if batch:
                try:
                    cur = col.insert_many(batch, ordered=False)
                    r += len(batch)
                    w += len(cur.inserted_ids)
                    print(f"{r} items read, {w} documents written...")
                except pymongo.errors.BulkWriteError as err:
                    print(f"{str(err):.400}... <snip>")
                    sys.exit()
            else:
                return w


#======================#
#   Collection tools   #
#======================#
def get_collection(db_col):
    db, collection = db_col.split(".")
    client = pymongo.MongoClient()
    col = client[db][collection]
    return col

# # Dumps a collection into a json file
# def dump_collection(db_col, fn):
#     col = get_collection(db_col)
#     print(f"Writing {col.count()} documents to {fn}...")
#     with jsonstreams.Stream(jsonstreams.Type.array, filename=fn) as s:
#         for e in col.find():
#             del e["_id"]
#             s.write(e)
#     print(f"Write complete.")

# Linebreaks aren't allowed in JSON, but sometimes this slips through
# Removing linebreaks should be safe, since JSON only treats them as formatting
def sanitise_json(src_fn):
    print(f"Sanitising {src_fn}...")
    clean_fn = re.sub("\.([^\.]+)$", r"-sanitised.\1", src_fn)
    with open(src_fn, "r") as rf, open(clean_fn, "w") as wf:
        for l in rf:
            wf.write(l.strip())
    return clean_fn
