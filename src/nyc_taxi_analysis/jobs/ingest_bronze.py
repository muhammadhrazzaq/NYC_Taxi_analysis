from pyspark.sql import SparkSession, functions as F, types as T

spark = SparkSession.builder.appName("ingest-bronze").getOrCreate()

RAW = "data/raw"

def ensure_table(df, table, partition_cols=None):
    cols = ", ".join(f"`{f.name}` {f.dataType.simpleString()}" for f in df.schema.fields)
    partition = f"PARTITIONED BY ({', '.join(partition_cols)})" if partition_cols else ""
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table} ({cols}) USING iceberg {partition}")
    
## schema enforcement
trip_schema = T.StructType([
    T.StructField("VendorID", T.IntegerType()),
    T.StructField("tpep_pickup_datetime", T.TimestampType()),
    T.StructField("tpep_dropoff_datetime", T.TimestampType()),
    T.StructField("passenger_count", T.IntegerType()),
    T.StructField("trip_distance", T.DoubleType()),
    T.StructField("RatecodeID", T.IntegerType()),
    T.StructField("store_and_fwd_flag", T.StringType()),
    T.StructField("PULocationID", T.IntegerType()),
    T.StructField("DOLocationID", T.IntegerType()),
    T.StructField("payment_type", T.IntegerType()),
    T.StructField("fare_amount", T.DoubleType()),
    T.StructField("extra", T.DoubleType()),
    T.StructField("mta_tax", T.DoubleType()),
    T.StructField("tip_amount", T.DoubleType()),
    T.StructField("tolls_amount", T.DoubleType()),
    T.StructField("improvement_surcharge", T.DoubleType()),
    T.StructField("total_amount", T.DoubleType()),
    T.StructField("congestion_surcharge", T.DoubleType()),
    T.StructField("_corrupt", T.StringType()),
])

raw = (spark.read
       .option("header", True)
       .option("mode", "PERMISSIVE")
       .option("columnNameOfCorruptRecord", "_corrupt")
       .schema(trip_schema)
       .csv(f"{RAW}/yellow_tripdata_*.csv"))

file_month = r"(\d{4})-(\d{2})\.csv$"

trips = (raw
         .withColumn("source_file", F.col("_metadata.file_path"))
         .withColumn("year",  F.regexp_extract("source_file", file_month, 1).cast("int"))
         .withColumn("month", F.regexp_extract("source_file", file_month, 2).cast("int"))
         .withColumn("ingested_at", F.current_timestamp()))
trips.cache()

good = trips.filter(F.col("_corrupt").isNull()).drop("_corrupt")
bad  = trips.filter(F.col("_corrupt").isNotNull()).select("_corrupt", "source_file", "year", "month", "ingested_at")

zones = (spark.read.option("header", True)
         .schema("LocationID INT, Borough STRING, Zone STRING, service_zone STRING")
         .csv(f"{RAW}/reference/taxi+_zone_lookup.csv")
         .withColumn("ingested_at", F.current_timestamp()))

ensure_table(good, "bronze.trips", ["year", "month"])
good.writeTo("bronze.trips").overwritePartitions()

ensure_table(bad, "bronze.trips_quarantine", ["year", "month"])
bad.writeTo("bronze.trips_quarantine").overwritePartitions()

ensure_table(zones, "bronze.zones")
zones.writeTo("bronze.zones").overwritePartitions()

(spark.table("bronze.trips")
      .groupBy("year", "month").count()
      .orderBy("year", "month")
      .show(30))

print("quarantined:", spark.table("bronze.trips_quarantine").count())
print("zones:", spark.table("bronze.zones").count())   # expect 265

spark.stop()