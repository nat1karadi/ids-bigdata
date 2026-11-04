from pyspark.sql import SparkSession, functions as F, Row
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, Bucketizer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator

s = (SparkSession.builder.master("spark://spark-master:7077")
     .appName("experiment_b").config("spark.executor.memory", "4g")
     .config("spark.sql.shuffle.partitions", "32").getOrCreate())
s.sparkContext.setLogLevel("ERROR")
base = "hdfs://namenode:8020/ids/"
CONT = ["rate", "ttl", "avg_pkt_size"]
FS = {"all": ["rate", "ttl", "avg_pkt_size", "proto_tcp", "proto_udp"],
      "no_rate": ["ttl", "avg_pkt_size", "proto_tcp", "proto_udp"],
      "no_ttl": ["rate", "avg_pkt_size", "proto_tcp", "proto_udp"],
      "no_size": ["rate", "ttl", "proto_tcp", "proto_udp"],
      "no_proto": ["rate", "ttl", "avg_pkt_size"]}

def prep(df, mode):
    d = df.withColumn("rate", F.log1p("rate")).withColumn("avg_pkt_size", F.log1p("avg_pkt_size"))
    if mode == "log_std":
        st = d.agg(*[F.mean(c).alias("m_" + c) for c in CONT],
                   *[F.stddev(c).alias("s_" + c) for c in CONT]).first()
        for c in CONT:
            d = d.withColumn(c, (F.col(c) - F.lit(st["m_" + c])) / F.lit(st["s_" + c] or 1.0))
        return d
    for c in CONT:  # rank: percentile of each value within its own dataset
        qs = sorted(set(d.approxQuantile(c, [i / 100 for i in range(1, 100)], 0.001)))
        splits = [float("-inf")] + qs + [float("inf")]
        d = (Bucketizer(splits=splits, inputCol=c, outputCol=c + "_b").transform(d)
             .withColumn(c, F.col(c + "_b") / F.lit(float(len(splits) - 2))).drop(c + "_b"))
    return d

def balance(df, seed=42):
    n = {r["is_attack"]: r["count"] for r in df.groupBy("is_attack").count().collect()}
    lo = min(n.values())
    return df.sampleBy("is_attack", {k: lo / v for k, v in n.items()}, seed).cache()

def metrics(pred):
    c = {(r["is_attack"], int(r["prediction"])): r["count"]
         for r in pred.groupBy("is_attack", "prediction").count().collect()}
    tp, fn, fp, tn = c.get((1, 1), 0), c.get((1, 0), 0), c.get((0, 1), 0), c.get((0, 0), 0)
    div = lambda a, b: a / b if b else 0.0
    return dict(bal_acc=(div(tn, tn + fp) + div(tp, tp + fn)) / 2,
                attack_recall=div(tp, tp + fn), benign_recall=div(tn, tn + fp))

ci_raw = s.read.parquet(base + "harmonised/ciciot2023")
un_raw = s.read.parquet(base + "harmonised/unsw")

print("Median feature values by class (0 = benign, 1 = attack)")
for name, df in [("CICIoT2023", ci_raw), ("UNSW-NB15", un_raw)]:
    print(name)
    df.groupBy("is_attack").agg(
        F.count("*").alias("rows"),
        F.percentile_approx("rate", 0.5).alias("rate"),
        F.percentile_approx("ttl", 0.5).alias("ttl"),
        F.percentile_approx("avg_pkt_size", 0.5).alias("avg_pkt_size"),
        F.round(F.mean("proto_tcp"), 3).alias("tcp"),
        F.round(F.mean("proto_udp"), 3).alias("udp")).orderBy("is_attack").show()

ev = BinaryClassificationEvaluator(labelCol="is_attack", rawPredictionCol="rawPrediction",
                                   metricName="areaUnderROC")
rows = []
for mode in ["log_std", "rank"]:
    ci, un = prep(ci_raw, mode), prep(un_raw, mode)
    ci_tr, ci_te = ci.randomSplit([0.7, 0.3], seed=42)
    un_tr, un_te = un.filter("split = 'train'"), un.filter("split = 'test'")
    ci_bal, un_bal = balance(ci_tr), balance(un_tr)
    ci_te, un_te = ci_te.cache(), un_te.cache()
    plan = [("within_CIC", ci_bal, ci_te), ("within_UNSW", un_bal, un_te),
            ("UNSW_to_CIC", un_bal, ci_te), ("CIC_to_UNSW", ci_bal, un_te)]
    for fs, cols in FS.items():
        for exp, train, test in plan:
            rf = RandomForestClassifier(featuresCol="features", labelCol="is_attack",
                                        numTrees=30, maxDepth=8, seed=42)
            fit = Pipeline(stages=[VectorAssembler(inputCols=cols, outputCol="features"), rf]).fit(train)
            pred = fit.transform(test).cache()
            m = metrics(pred)
            auc = ev.evaluate(pred)
            pred.unpersist()
            rows.append(Row(preprocessing=mode, features=fs, experiment=exp, auc=auc, **m))
            print(f"{mode:8s} {fs:9s} {exp:12s} balAcc={m['bal_acc']:.3f} auc={auc:.3f} "
                  f"attackRecall={m['attack_recall']:.3f} benignRecall={m['benign_recall']:.3f}", flush=True)

print("\nSummary (balanced accuracy / AUC)")
g = {(r.preprocessing, r.features, r.experiment): r for r in rows}
for mode in ["log_std", "rank"]:
    for fs in FS:
        a, b = g[(mode, fs, "UNSW_to_CIC")], g[(mode, fs, "CIC_to_UNSW")]
        print(f"{mode:8s} {fs:9s} UNSW_to_CIC {a.bal_acc:.3f}/{a.auc:.3f}   CIC_to_UNSW {b.bal_acc:.3f}/{b.auc:.3f}")

s.createDataFrame(rows).coalesce(1).write.mode("overwrite").json(base + "results/experiment_b")
s.stop()
