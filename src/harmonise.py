from pyspark.sql import SparkSession, functions as F

s = (SparkSession.builder.master("spark://spark-master:7077")
     .appName("harmonise").config("spark.executor.memory", "4g")
     .config("spark.sql.shuffle.partitions", "64").getOrCreate())
s.sparkContext.setLogLevel("ERROR")
base = "hdfs://namenode:8020/ids/"

V1 = ["rate", "ttl", "avg_pkt_size", "proto_tcp", "proto_udp"]
EXTRA = ["proto_icmp", "proto_other", "app_http", "app_dns", "iat_ms"]
NUM = ["rate", "ttl", "avg_pkt_size", "iat_ms"]

def finite(c):
    x = F.col(c).cast("double")
    return x.isNotNull() & (~F.isnan(x)) & (x > F.lit(-1e308)) & (x < F.lit(1e308))

def keep_finite(df):
    cond = F.lit(True)
    for c in NUM:
        cond = cond & finite(c)
    return df.filter(cond)

def final(df):
    cols = ["dataset", "split", "label_raw", "unified_class", "is_attack"] + V1 + EXTRA
    return df.select(*cols)

def report(df, name):
    n = df.count()
    g = df.groupBy(V1).agg(F.count("*").alias("n"), F.countDistinct("is_attack").alias("k"))
    conflict_rows = g.filter("k > 1").agg(F.sum("n")).first()[0] or 0
    print(name, "| rows:", n, "| distinct feature vectors:", g.count(),
          "| rows in vectors with both benign and attack:", conflict_rows)
    df.groupBy("unified_class").count().orderBy("count", ascending=False).show()

# CICIoT2023
ci = s.read.parquet(base + "clean/ciciot2023")
lab = F.col("label")
ci = (ci.withColumn("dataset", F.lit("ciciot2023")).withColumn("split", F.lit("all"))
      .withColumn("label_raw", lab)
      .withColumn("unified_class",
                  F.when(lab == "BENIGN", "BENIGN")
                   .when(lab.startswith("DDOS-") | lab.startswith("DOS-") | lab.startswith("MIRAI-"), "DOS_DDOS")
                   .when(lab.startswith("RECON-") | (lab == "VULNERABILITYSCAN"), "RECON")
                   .otherwise("OTHER_ATTACK"))
      .withColumn("rate", F.col("rate").cast("double"))
      .withColumn("ttl", F.col("time_to_live").cast("double"))
      .withColumn("avg_pkt_size", F.col("avg").cast("double"))
      .withColumn("proto_tcp", (F.col("protocol_type") == 6).cast("int"))
      .withColumn("proto_udp", (F.col("protocol_type") == 17).cast("int"))
      .withColumn("proto_icmp", (F.col("protocol_type") == 1).cast("int"))
      .withColumn("proto_other", (~F.col("protocol_type").isin(6, 17, 1)).cast("int"))
      .withColumn("app_http", (F.col("http") > 0).cast("int"))
      .withColumn("app_dns", (F.col("dns") > 0).cast("int"))
      .withColumn("iat_ms", F.col("iat").cast("double") * 1000.0))
n_ci = ci.count()
ci = final(keep_finite(ci))
print("CICIoT2023 rows removed for non-finite values:", n_ci - ci.count())

# UNSW-NB15 (remove test rows that also appear in train)
tr = s.read.parquet(base + "clean/unsw_train")
te = s.read.parquet(base + "clean/unsw_test")
n_te = te.count()
te = te.subtract(tr)
print("UNSW test rows removed (identical to a train row):", n_te - te.count())

def unsw(df, split):
    a = F.col("attack_cat")
    df = (df.withColumn("dataset", F.lit("unsw")).withColumn("split", F.lit(split))
          .withColumn("label_raw", a)
          .withColumn("is_attack", F.col("label").cast("int"))
          .withColumn("unified_class",
                      F.when(a == "Normal", "BENIGN").when(a == "DoS", "DOS_DDOS")
                       .when(a == "Reconnaissance", "RECON").otherwise("OTHER_ATTACK"))
          .withColumn("rate", F.col("rate").cast("double"))
          .withColumn("ttl", F.col("sttl").cast("double"))
          .withColumn("avg_pkt_size", (F.col("sbytes") + F.col("dbytes")).cast("double") /
                      F.when((F.col("spkts") + F.col("dpkts")) > 0, F.col("spkts") + F.col("dpkts")))
          .withColumn("proto_tcp", (F.col("proto") == "tcp").cast("int"))
          .withColumn("proto_udp", (F.col("proto") == "udp").cast("int"))
          .withColumn("proto_icmp", (F.col("proto") == "icmp").cast("int"))
          .withColumn("proto_other", (~F.col("proto").isin("tcp", "udp", "icmp")).cast("int"))
          .withColumn("app_http", (F.col("service") == "http").cast("int"))
          .withColumn("app_dns", (F.col("service") == "dns").cast("int"))
          .withColumn("iat_ms", F.col("sinpkt").cast("double")))
    return df

un = final(keep_finite(unsw(tr, "train"))).unionByName(final(keep_finite(unsw(te, "test"))))

ci.write.mode("overwrite").parquet(base + "harmonised/ciciot2023")
un.write.mode("overwrite").parquet(base + "harmonised/unsw")

ci = s.read.parquet(base + "harmonised/ciciot2023")
un = s.read.parquet(base + "harmonised/unsw")
report(ci, "CICIoT2023")
report(un, "UNSW-NB15")
print("Feature medians by dataset (v1 features)")
for name, df in [("CICIoT2023", ci), ("UNSW-NB15", un)]:
    print(name)
    df.select(V1).summary("min", "50%", "max").show()
s.stop()
