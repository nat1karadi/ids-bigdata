from pyspark.sql import SparkSession
s = SparkSession.builder.master("spark://spark-master:7077").appName("inspect").getOrCreate()
s.sparkContext.setLogLevel("ERROR")

ci = s.read.csv("hdfs://namenode:8020/ids/raw/ciciot2023/*.csv", header=True, inferSchema=True)
print("CICIoT2023 rows:", ci.count(), "columns:", len(ci.columns))
print(ci.columns)
ci.groupBy("label").count().orderBy("count", ascending=False).show(40, truncate=False)

for name in ["training-set", "testing-set"]:
    u = s.read.csv(f"hdfs://namenode:8020/ids/raw/unsw/UNSW_NB15_{name}.csv", header=True, inferSchema=True)
    print("UNSW", name, "rows:", u.count(), "columns:", len(u.columns))
    print(u.columns)
    u.groupBy("attack_cat").count().orderBy("count", ascending=False).show(truncate=False)
s.stop()
