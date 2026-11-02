from pyspark.sql import SparkSession, functions as F
s = (SparkSession.builder.master("spark://spark-master:7077")
     .appName("verify").config("spark.executor.memory", "4g").getOrCreate())
s.sparkContext.setLogLevel("ERROR")
base = "hdfs://namenode:8020/ids/clean/"
ci = s.read.parquet(base + "ciciot2023")
tr = s.read.parquet(base + "unsw_train")
te = s.read.parquet(base + "unsw_test")

print("CICIoT2023 protocol_type values")
ci.groupBy("protocol_type").count().orderBy("count", ascending=False).show(10)
print("CICIoT2023 shared-feature summary")
ci.select("number", "rate", "iat", "avg", "time_to_live").summary("min", "50%", "max").show()
print("CICIoT2023 share of rows with protocol present")
ci.select([F.mean((F.col(c) > 0).cast("int")).alias(c)
           for c in ["http", "https", "dns", "ssh", "smtp", "irc", "tcp", "udp", "icmp"]]).show()

print("UNSW proto and service")
tr.groupBy("proto").count().orderBy("count", ascending=False).show(8)
tr.groupBy("service").count().orderBy("count", ascending=False).show(8)
print("UNSW shared-feature summary")
tr.select("dur", "rate", "sttl", "sinpkt", "smean", "dmean").summary("min", "50%", "max").show()
print("UNSW rows with dur = 0:", tr.filter("dur = 0").count())
tr.select("dur", "spkts", "dpkts", "rate").show(5)

print("Identical rows in both UNSW train and test:", tr.intersect(te).count())
feat = [c for c in ci.columns if c not in ("label", "is_attack")]
conf = ci.groupBy(feat).agg(F.countDistinct("label").alias("n")).filter("n > 1").count()
print("CICIoT2023 feature vectors with conflicting labels:", conf)
s.stop()
