import json
from pymongo import MongoClient

db = MongoClient("localhost", 27017).ids

def rows(path):
    with open(path) as f:
        return [json.loads(line) for line in f if line.strip()]

docs = []
for letter in "abcd":
    for r in rows(f"results/experiment_{letter}.jsonl"):
        if isinstance(r.get("metrics"), str):
            r.update(json.loads(r.pop("metrics")))
        if "direction" in r:
            r["experiment"] = r["direction"]
        r["experiment_set"] = letter.upper()
        docs.append(r)

db.experiment_results.drop()
db.scale_runs.drop()
db.experiment_results.insert_many(docs)
db.scale_runs.insert_many(rows("results/scale_results.jsonl"))
db.experiment_results.create_index([("experiment_set", 1), ("experiment", 1)])
db.scale_runs.create_index([("n_files", 1), ("cores", 1)])

print("experiment_results:", db.experiment_results.count_documents({}))
print("scale_runs:", db.scale_runs.count_documents({}))

print("\nExperiment D: mean balanced accuracy by labelled target rows")
for r in db.experiment_results.aggregate([
        {"$match": {"experiment_set": "D"}},
        {"$group": {"_id": {"d": "$direction", "f": "$fraction"},
                    "bal_acc": {"$avg": "$bal_acc"}, "n": {"$avg": "$n_target"}}},
        {"$sort": {"_id.d": 1, "_id.f": 1}}]):
    print(f"{r['_id']['d']:12s} rows~{int(r['n']):6d} balAcc={r['bal_acc']:.3f}")

print("\nScalability runs, fastest first")
for r in db.scale_runs.find({}, {"_id": 0}).sort("t_total", 1):
    print(r)
