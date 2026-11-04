import json
import statistics as stats
from pyspark.sql import SparkSession, functions as F, Row
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator

s = (SparkSession.builder.master("spark://spark-master:7077")
     .appName("experiment_c").config("spark.executor.memory", "4g")
     .config("spark.sql.shuffle.partitions", "32").getOrCreate())
s.sparkContext.setLogLevel("ERROR")
base = "hdfs://namenode:8020/ids/"
V1 = ["rate", "ttl", "avg_pkt_size", "proto_tcp", "proto_udp"]
CONT = ["rate", "ttl", "avg_pkt_size"]
SEEDS = [1, 2, 3]
ev = BinaryClassificationEvaluator(labelCol="is_attack", rawPredictionCol="rawPrediction",
                                   metricName="areaUnderROC")

def prep(df, mode):
    if mode == "naive":
        return df
    d = df.withColumn("rate", F.log1p("rate")).withColumn("avg_pkt_size", F.log1p("avg_pkt_size"))
    t = d.agg(*[F.mean(c).alias("m_" + c) for c in CONT],
              *[F.stddev(c).alias("s_" + c) for c in CONT]).first()
    for c in CONT:
        d = d.withColumn(c, (F.col(c) - F.lit(t["m_" + c])) / F.lit(t["s_" + c] or 1.0))
    return d

def add_y(df):
    y = (F.when(F.col("unified_class") == "BENIGN", 0)
          .when(F.col("unified_class") == "DOS_DDOS", 1)
          .when(F.col("unified_class") == "RECON", 2))
    return df.withColumn("y", y).filter("y is not null")

def balance(df, col, seed):
    n = {r[col]: r["count"] for r in df.groupBy(col).count().collect()}
    lo = min(n.values())
    return df.sampleBy(col, {k: lo / v for k, v in n.items()}, seed).cache()

div = lambda a, b: a / b if b else 0.0

def bin_metrics(pred):
    c = {(r["is_attack"], int(r["prediction"])): r["count"]
         for r in pred.groupBy("is_attack", "prediction").count().collect()}
    tp, fn, fp, tn = c.get((1, 1), 0), c.get((1, 0), 0), c.get((0, 1), 0), c.get((0, 0), 0)
    return dict(bal_acc=(div(tn, tn + fp) + div(tp, tp + fn)) / 2,
                attack_recall=div(tp, tp + fn), benign_recall=div(tn, tn + fp))

def mc_metrics(pred, k=3):
    cm = {(r["y"], int(r["prediction"])): r["count"]
          for r in pred.groupBy("y", "prediction").count().collect()}
    rec, f1s = [], []
    for i in range(k):
        tp = cm.get((i, i), 0)
        fn = sum(cm.get((i, j), 0) for j in range(k) if j != i)
        fp = sum(cm.get((j, i), 0) for j in range(k) if j != i)
        r, p = div(tp, tp + fn), div(tp, tp + fp)
        rec.append(r)
        f1s.append(div(2 * p * r, p + r))
    return dict(macro_recall=sum(rec) / k, macro_f1=sum(f1s) / k,
                rec_benign=rec[0], rec_dos=rec[1], rec_recon=rec[2])

def fit_binary(train, test, seed):
    rf = RandomForestClassifier(featuresCol="features", labelCol="is_attack",
                                numTrees=30, maxDepth=8, seed=seed)
    fit = Pipeline(stages=[VectorAssembler(inputCols=V1, outputCol="features"), rf]).fit(train)
    pred = fit.transform(test).cache()
    m = bin_metrics(pred)
    m["auc"] = ev.evaluate(pred)
    pred.unpersist()
    return m

def fit_multi(train, test, seed):
    rf = RandomForestClassifier(featuresCol="features", labelCol="y",
                                numTrees=30, maxDepth=8, seed=seed)
    fit = Pipeline(stages=[VectorAssembler(inputCols=V1, outputCol="features"), rf]).fit(train)
    pred = fit.transform(test).cache()
    m = mc_metrics(pred)
    pred.unpersist()
    return m

ci_raw = s.read.parquet(base + "harmonised/ciciot2023")
un_raw = s.read.parquet(base + "harmonised/unsw")
P = {mode: (prep(ci_raw, mode), prep(un_raw, mode)) for mode in ["naive", "log_std"]}
results = []

def record(task, mode, exp, seed, m):
    results.append(dict(task=task, mode=mode, exp=exp, seed=seed, m=m))
    print(f"seed={seed} {task:10s} {mode:8s} {exp:14s} " +
          " ".join(f"{k}={v:.3f}" for k, v in m.items()), flush=True)

for seed in SEEDS:
    for mode in ["naive", "log_std"]:
        ci, un = P[mode]
        ci_tr, ci_te = ci.randomSplit([0.7, 0.3], seed=seed)
        un_tr, un_te = un.filter("split = 'train'"), un.filter("split = 'test'")
        ci_b, un_b = balance(ci_tr, "is_attack", seed), balance(un_tr, "is_attack", seed)
        ci_te, un_te = ci_te.cache(), un_te.cache()
        plan = [("within_CIC", ci_b, ci_te), ("within_UNSW", un_b, un_te),
                ("UNSW_to_CIC", un_b, ci_te), ("CIC_to_UNSW", ci_b, un_te)]
        for exp, tr, te in plan:
            record("binary", mode, exp, seed, fit_binary(tr, te, seed))
        if mode == "log_std":
            joint = ci_b.unionByName(un_b)
            record("joint", mode, "joint_on_CIC", seed, fit_binary(joint, ci_te, seed))
            record("joint", mode, "joint_on_UNSW", seed, fit_binary(joint, un_te, seed))
            cm, um = add_y(ci), add_y(un)
            cm_tr, cm_te = cm.randomSplit([0.7, 0.3], seed=seed)
            um_tr, um_te = um.filter("split = 'train'"), um.filter("split = 'test'")
            cm_b, um_b = balance(cm_tr, "y", seed), balance(um_tr, "y", seed)
            cm_te, um_te = cm_te.cache(), um_te.cache()
            plan_m = [("within_CIC", cm_b, cm_te), ("within_UNSW", um_b, um_te),
                      ("UNSW_to_CIC", um_b, cm_te), ("CIC_to_UNSW", cm_b, um_te)]
            for exp, tr, te in plan_m:
                record("multiclass", mode, exp, seed, fit_multi(tr, te, seed))
    s.catalog.clearCache()

print("\nMean +/- sd over", len(SEEDS), "seeds")
groups = {}
for r in results:
    groups.setdefault((r["task"], r["mode"], r["exp"]), []).append(r["m"])
for (task, mode, exp), ms in groups.items():
    parts = [f"{k}={stats.mean([m[k] for m in ms]):.3f}+/-{stats.stdev([m[k] for m in ms]):.3f}"
             for k in ms[0]]
    print(f"{task:10s} {mode:8s} {exp:14s} " + " ".join(parts))

s.createDataFrame([Row(task=r["task"], preprocessing=r["mode"], experiment=r["exp"],
                       seed=r["seed"], metrics=json.dumps(r["m"])) for r in results]) \
    .coalesce(1).write.mode("overwrite").json(base + "results/experiment_c")
s.stop()
