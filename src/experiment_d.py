import statistics as stats
from pyspark.sql import SparkSession, functions as F, Row
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator

s = (SparkSession.builder.master("spark://spark-master:7077")
     .appName("experiment_d").config("spark.executor.memory", "4g")
     .config("spark.sql.shuffle.partitions", "32").getOrCreate())
s.sparkContext.setLogLevel("ERROR")
base = "hdfs://namenode:8020/ids/"
V1 = ["rate", "ttl", "avg_pkt_size", "proto_tcp", "proto_udp"]
CONT = ["rate", "ttl", "avg_pkt_size"]
SEEDS = [1, 2, 3]
FRACTIONS = [0.0, 0.001, 0.005, 0.02, 0.1]
ev = BinaryClassificationEvaluator(labelCol="is_attack", rawPredictionCol="rawPrediction",
                                   metricName="areaUnderROC")

def prep(df):
    d = df.withColumn("rate", F.log1p("rate")).withColumn("avg_pkt_size", F.log1p("avg_pkt_size"))
    t = d.agg(*[F.mean(c).alias("m_" + c) for c in CONT],
              *[F.stddev(c).alias("s_" + c) for c in CONT]).first()
    for c in CONT:
        d = d.withColumn(c, (F.col(c) - F.lit(t["m_" + c])) / F.lit(t["s_" + c] or 1.0))
    return d

def balance(df, seed):
    n = {r["is_attack"]: r["count"] for r in df.groupBy("is_attack").count().collect()}
    lo = min(n.values())
    return df.sampleBy("is_attack", {k: lo / v for k, v in n.items()}, seed).cache()

div = lambda a, b: a / b if b else 0.0

def evaluate(pred):
    c = {(r["is_attack"], int(r["prediction"])): r["count"]
         for r in pred.groupBy("is_attack", "prediction").count().collect()}
    tp, fn, fp, tn = c.get((1, 1), 0), c.get((1, 0), 0), c.get((0, 1), 0), c.get((0, 0), 0)
    return dict(bal_acc=(div(tn, tn + fp) + div(tp, tp + fn)) / 2,
                benign_recall=div(tn, tn + fp), attack_recall=div(tp, tp + fn))

ci = prep(s.read.parquet(base + "harmonised/ciciot2023"))
un = prep(s.read.parquet(base + "harmonised/unsw"))
results = []
for seed in SEEDS:
    ci_tr, ci_te = ci.randomSplit([0.7, 0.3], seed=seed)
    un_tr, un_te = un.filter("split = 'train'"), un.filter("split = 'test'")
    ci_b, un_b = balance(ci_tr, seed), balance(un_tr, seed)
    ci_te, un_te = ci_te.cache(), un_te.cache()
    for direction, src, tgt, test in [("UNSW_to_CIC", un_b, ci_b, ci_te),
                                      ("CIC_to_UNSW", ci_b, un_b, un_te)]:
        n_s = src.count()
        for f in FRACTIONS:
            train = src.withColumn("w", F.lit(1.0))
            n_t = 0
            if f > 0:
                t = tgt.sample(False, f, seed)
                n_t = t.count()
                train = train.unionByName(t.withColumn("w", F.lit(n_s / max(n_t, 1))))
            rf = RandomForestClassifier(featuresCol="features", labelCol="is_attack",
                                        weightCol="w", numTrees=30, maxDepth=8, seed=seed)
            fit = Pipeline(stages=[VectorAssembler(inputCols=V1, outputCol="features"), rf]).fit(train)
            pred = fit.transform(test).cache()
            m = evaluate(pred)
            m["auc"] = ev.evaluate(pred)
            pred.unpersist()
            results.append(dict(seed=seed, direction=direction, fraction=f, n_target=n_t, m=m))
            print(f"seed={seed} {direction} labelled_target_rows={n_t:6d} " +
                  " ".join(f"{k}={v:.3f}" for k, v in m.items()), flush=True)
    s.catalog.clearCache()

print("\nMean +/- sd over", len(SEEDS), "seeds")
for direction in ["UNSW_to_CIC", "CIC_to_UNSW"]:
    for f in FRACTIONS:
        rs = [r for r in results if r["direction"] == direction and r["fraction"] == f]
        n = int(stats.mean([r["n_target"] for r in rs]))
        parts = [f"{k}={stats.mean([r['m'][k] for r in rs]):.3f}+/-{stats.stdev([r['m'][k] for r in rs]):.3f}"
                 for k in ["bal_acc", "auc", "benign_recall", "attack_recall"]]
        print(f"{direction} labelled_target_rows~{n:6d} " + " ".join(parts))

s.createDataFrame([Row(direction=r["direction"], seed=r["seed"], fraction=r["fraction"],
                       n_target=r["n_target"], **r["m"]) for r in results]) \
    .coalesce(1).write.mode("overwrite").json(base + "results/experiment_d")
s.stop()
