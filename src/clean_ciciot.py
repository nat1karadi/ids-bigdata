import re
from pyspark.sql import SparkSession, functions as F

s = (SparkSession.builder.master("spark://spark-master:7077")
     .appName("clean_ciciot").config("spark.executor.memory", "4g")
     .config("spark.sql.shuffle.partitions", "64")
     .getOrCreate())
s.sparkContext.setLogLevel("ERROR")

df = s.read.csv("hdfs://namenode:8020/ids/raw/ciciot2023/*.csv", header=True, inferSchema=True)
for c in df.columns:
    df = df.withColumnRenamed(c, re.sub(r"\W+", "_", c.strip()).lower())

n0 = df.count()
df = df.dropDuplicates()
n1 = df.count()

num_cols = [f.name for f in df.schema.fields
            if f.dataType.typeName() in ("double", "float", "integer", "long")]
bad = F.lit(False)
for c in num_cols:
    x = F.col(c).cast("double")
    bad = bad | x.isNull() | F.isnan(x) | x.isin(float("inf"), float("-inf"))
df = df.filter(~bad)
n2 = df.count()

df = df.withColumn("label", F.upper(F.trim(F.col("label"))))
df = df.withColumn("is_attack", (F.col("label") != "BENIGN").cast("int"))

df.write.mode("overwrite").parquet("hdfs://namenode:8020/ids/clean/ciciot2023")

print("rows raw:", n0, "| after duplicates removed:", n1, "| after bad values removed:", n2)
df.groupBy("is_attack").count().show()
df.groupBy("label").count().orderBy("count", ascending=False).show(40, truncate=False)
s.stop()
