import re
from pyspark.sql import SparkSession, functions as F

s = (SparkSession.builder.master("spark://spark-master:7077")
     .appName("clean_unsw").config("spark.executor.memory", "4g")
     .config("spark.sql.shuffle.partitions", "32")
     .getOrCreate())
s.sparkContext.setLogLevel("ERROR")

for name, out in [("training-set", "unsw_train"), ("testing-set", "unsw_test")]:
    df = s.read.csv(f"hdfs://namenode:8020/ids/raw/unsw/UNSW_NB15_{name}.csv",
                    header=True, inferSchema=True)
    df = df.drop("id")
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
    df = df.withColumn("attack_cat", F.trim(F.col("attack_cat")))
    n2 = df.count()

    df.write.mode("overwrite").parquet(f"hdfs://namenode:8020/ids/clean/{out}")
    print(name, "rows raw:", n0, "| after duplicates removed:", n1, "| after bad values removed:", n2)
    df.groupBy("label").count().show()
    df.groupBy("attack_cat").count().orderBy("count", ascending=False).show(truncate=False)
s.stop()
