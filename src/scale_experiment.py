import time
import sys
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier

def run_experiment(data_path, num_cores):
    spark = SparkSession.builder \
        .appName(f"IDSScalabilityTest-Cores{num_cores}") \
        .master(f"local[{num_cores}]") \
        .getOrCreate()
    
    start_time = time.time()
    
    # 1. Load CSVs directly from HDFS
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(data_path)
    
    # 2. Basic cleaning (drop nulls)
    df_clean = df.na.drop()
    
    # 3. Assemble features
    numeric_cols = [c for c, t in df_clean.dtypes if t in ('int', 'double', 'float', 'bigint') and c != 'label']
    assembler = VectorAssembler(inputCols=numeric_cols[:10], outputCol="raw_features")
    assembled = assembler.transform(df_clean)
    
    scaler = StandardScaler(inputCol="raw_features", outputCol="features", withStd=True, withMean=True)
    scaled_model = scaler.fit(assembled)
    scaled_data = scaled_model.transform(assembled)
    
    # 4. Train a lightweight Random Forest model
    label_col = "label" if "label" in df_clean.columns else df_clean.columns[-1]
    rf = RandomForestClassifier(labelCol=label_col, featuresCol="features", numTrees=10, maxDepth=5)
    model = rf.fit(scaled_data.limit(50000))
    
    duration = time.time() - start_time
    spark.stop()
    return duration

if __name__ == "__main__":
    cores = int(sys.argv[1]) if len(sys.argv) > 1 else 2
    path = "hdfs://namenode:8020/ids/raw/ciciot2023_scale/*.csv"
    print(f"Running scalability test with {cores} core(s)...")
    elapsed = run_experiment(path, cores)
    print(f"RESULT_DURATION_SECONDS,{cores},{elapsed:.2f}")
