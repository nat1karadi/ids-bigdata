from pyspark.sql import SparkSession
s = SparkSession.builder.master("spark://spark-master:7077").appName("smoke").getOrCreate()
s.read.csv("hdfs://namenode:8020/ids/raw/test.csv", header=True).show()
s.stop()
