from pyspark.sql import SparkSession, functions as F, Row
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier

s = (SparkSession.builder.master("spark://spark-master:7077")
     .appName("experiment_a").config("spark.executor.memory", "4g")
     .config("spark.sql.shuffle.partitions", "32").getOrCreate())
s.sparkContext.setLogLevel("ERROR")
base = "hdfs://namenode:8020/ids/"
V1 = ["rate", "ttl", "avg_pkt_size", "proto_tcp", "proto_udp"]
CONT = ["rate", "ttl", "avg_pkt_size"]

def prep(df, mode):
    if mode == "naive":
        return df
    d = df.withColumn("rate", F.log1p("rate")).withColumn("avg_pkt_size", F.log1p("avg_pkt_size"))
    st = d.agg(*[F.mean(c).alias("m_" + c) for c in CONT],
               *[F.stddev(c).alias("s_" + c) for c in CONT]).first()
    for c in CONT:
        sd = st["s_" + c] or 1.0
        d = d.withColumn(c, (F.col(c) - F.lit(st["m_" + c])) / F.lit(sd))
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
    f1 = lambda p, r: div(2 * p * r, p + r)
    rec1, pre1 = div(tp, tp + fn), div(tp, tp + fp)
    rec0, pre0 = div(tn, tn + fp), div(tn, tn + fn)
    return dict(macro_f1=(f1(pre1, rec1) + f1(pre0, rec0)) / 2, bal_acc=(rec0 + rec1) / 2,
                attack_recall=rec1, attack_precision=pre1, benign_recall=rec0,
                accuracy=div(tp + tn, tp + tn + fp + fn))

ci_raw = s.read.parquet(base + "harmonised/ciciot2023")
un_raw = s.read.parquet(base + "harmonised/unsw")
models = {
    "LR": lambda: LogisticRegression(featuresCol="features", labelCol="is_attack", maxIter=50),
    "RF": lambda: RandomForestClassifier(featuresCol="features", labelCol="is_attack",
                                         numTrees=50, maxDepth=10, seed=42),
}
rows = []
for mode in ["naive", "log_std"]:
    ci, un = prep(ci_raw, mode), prep(un_raw, mode)
    ci_tr, ci_te = ci.randomSplit([0.7, 0.3], seed=42)
    un_tr, un_te = un.filter("split = 'train'"), un.filter("split = 'test'")
    ci_bal, un_bal = balance(ci_tr), balance(un_tr)
    ci_te, un_te = ci_te.cache(), un_te.cache()
    plan = [("within_CIC", ci_bal, ci_te), ("within_UNSW", un_bal, un_te),
            ("UNSW_to_CIC", un_bal, ci_te), ("CIC_to_UNSW", ci_bal, un_te)]
    for mname, mk in models.items():
        for exp, train, test in plan:
            fit = Pipeline(stages=[VectorAssembler(inputCols=V1, outputCol="features"), mk()]).fit(train)
            m = metrics(fit.transform(test))
            rows.append(Row(experiment=exp, preprocessing=mode, model=mname, **m))
            print(f"{mode:8s} {mname} {exp:12s} macroF1={m['macro_f1']:.3f} balAcc={m['bal_acc']:.3f} "
                  f"attackRecall={m['attack_recall']:.3f} attackPrec={m['attack_precision']:.3f} "
                  f"benignRecall={m['benign_recall']:.3f}", flush=True)

print("\nGeneralisation gap (macro-F1, within minus cross)")
get = {(r.preprocessing, r.model, r.experiment): r.macro_f1 for r in rows}
for mode in ["naive", "log_std"]:
    for mname in models:
        print(f"{mode:8s} {mname}  CIC: {get[(mode, mname, 'within_CIC')] - get[(mode, mname, 'UNSW_to_CIC')]:.3f}"
              f"   UNSW: {get[(mode, mname, 'within_UNSW')] - get[(mode, mname, 'CIC_to_UNSW')]:.3f}")

s.createDataFrame(rows).coalesce(1).write.mode("overwrite").json(base + "results/experiment_a")
s.stop()
