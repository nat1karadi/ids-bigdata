import sys, json, time, re
from pyspark.sql import SparkSession, functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier

n_files, cores = int(sys.argv[1]), int(sys.argv[2])
s = (SparkSession.builder.master("spark://spark-master:7077")
     .appName(f"scale_{n_files}f_{cores}c")
     .config("spark.executor.memory", "4g")
     .config("spark.cores.max", str(cores))
     .config("spark.sql.shuffle.partitions", "64").getOrCreate())
s.sparkContext.setLogLevel("ERROR")
base = "hdfs://namenode:8020/ids/"
tag = f"{n_files}f_{cores}c"
files = [f"{base}raw/ciciot2023_scale/Merged{i:02d}.csv" for i in range(1, n_files + 1)]
tmp_clean, tmp_harm = f"{base}scale_tmp/clean_{tag}", f"{base}scale_tmp/harm_{tag}"

jvm, hconf = s._jvm, s._jsc.hadoopConfiguration()
hpath = lambda p: jvm.org.apache.hadoop.fs.Path(p)
fs_of = lambda p: hpath(p).getFileSystem(hconf)
size_bytes = lambda ps: sum(fs_of(p).getContentSummary(hpath(p)).getLength() for p in ps)
rm = lambda p: fs_of(p).delete(hpath(p), True)

# Stage 1: read, clean, write
t0 = time.time()
df = s.read.csv(files, header=True, inferSchema=True)
for c in df.columns:
    df = df.withColumnRenamed(c, re.sub(r"\W+", "_", c.strip()).lower())
df = df.select([F.col(c).cast("double").alias(c) if c != "label" else F.col(c) for c in df.columns])
df = df.dropDuplicates()
num = [f.name for f in df.schema.fields if f.dataType.typeName() in ("double", "float", "integer", "long")]
ok = F.lit(True)
for c in num:
    x = F.col(c).cast("double")
    ok = ok & x.isNotNull() & (~F.isnan(x)) & (x > F.lit(-1e308)) & (x < F.lit(1e308))
df.filter(ok).write.mode("overwrite").parquet(tmp_clean)
t_clean = time.time() - t0
rows_clean = s.read.parquet(tmp_clean).count()

# Stage 2: build the shared features
t0 = time.time()
d = s.read.parquet(tmp_clean)
lab = F.upper(F.trim(F.col("label")))
d = (d.withColumn("is_attack", (lab != "BENIGN").cast("int"))
      .withColumn("ttl", F.col("time_to_live").cast("double"))
      .withColumn("avg_pkt_size", F.col("avg").cast("double"))
      .withColumn("proto_tcp", (F.col("protocol_type") == 6).cast("int"))
      .withColumn("proto_udp", (F.col("protocol_type") == 17).cast("int"))
      .select("is_attack", F.col("rate").cast("double").alias("rate"),
              "ttl", "avg_pkt_size", "proto_tcp", "proto_udp"))
d.write.mode("overwrite").parquet(tmp_harm)
t_features = time.time() - t0

# Stage 3: balance, train a Random Forest, evaluate
t0 = time.time()
d = s.read.parquet(tmp_harm)
tr, te = d.randomSplit([0.7, 0.3], seed=1)
n = {r["is_attack"]: r["count"] for r in tr.groupBy("is_attack").count().collect()}
bal_acc = None
if len(n) == 2:
    lo = min(n.values())
    tr_b = tr.sampleBy("is_attack", {k: lo / v for k, v in n.items()}, 1)
    feats = ["rate", "ttl", "avg_pkt_size", "proto_tcp", "proto_udp"]
    rf = RandomForestClassifier(featuresCol="features", labelCol="is_attack",
                                numTrees=30, maxDepth=8, seed=1)
    fit = Pipeline(stages=[VectorAssembler(inputCols=feats, outputCol="features"), rf]).fit(tr_b)
    c = {(r["is_attack"], int(r["prediction"])): r["count"]
         for r in fit.transform(te).groupBy("is_attack", "prediction").count().collect()}
    tp, fn, fp, tn = c.get((1, 1), 0), c.get((1, 0), 0), c.get((0, 1), 0), c.get((0, 0), 0)
    bal_acc = (tn / max(tn + fp, 1) + tp / max(tp + fn, 1)) / 2
t_train = time.time() - t0

input_mb = size_bytes(files) / 1e6
rm(tmp_clean); rm(tmp_harm)
print("RESULT " + json.dumps(dict(
    n_files=n_files, cores=cores, input_mb=round(input_mb), rows_clean=rows_clean,
    t_clean=round(t_clean, 1), t_features=round(t_features, 1), t_train=round(t_train, 1),
    t_total=round(t_clean + t_features + t_train, 1),
    bal_acc=None if bal_acc is None else round(bal_acc, 3))))
s.stop()
