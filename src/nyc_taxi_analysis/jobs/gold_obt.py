from itertools import chain
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("gold-obt").getOrCreate()

def ensure_table(df, table, partition_cols=None):
    """
    Create a table in the Spark catalog if it doesn't exist, using the schema of the provided
    DataFrame. Optionally partition the table by the specified columns.
    """
    cols = ", ".join(f"`{f.name}` {f.dataType.simpleString()}" for f in df.schema.fields)
    partition = f"PARTITIONED BY ({', '.join(partition_cols)})" if partition_cols else ""
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table} ({cols}) USING iceberg {partition}")

def decode(col, mapping, default="Unknown"):
    """
    Decode a column using a mapping dictionary, returning a default value for unmapped values."""
    m = F.create_map(*[F.lit(x) for x in chain(*mapping.items())])
    return F.coalesce(m[F.col(col)], F.lit(default))

trips = spark.table("silver.trips")
zones = spark.table("bronze.zones").select("LocationID", "Borough", "Zone", "service_zone")

"""
Join zones twice (pickup and drop-off)
"""
pu = zones.select(F.col("LocationID").alias("PULocationID"),
                  F.col("Borough").alias("pu_borough"),
                  F.col("Zone").alias("pu_zone"),
                  F.col("service_zone").alias("pu_service_zone"))

do = zones.select(F.col("LocationID").alias("DOLocationID"),
                  F.col("Borough").alias("do_borough"),
                  F.col("Zone").alias("do_zone"),
                  F.col("service_zone").alias("do_service_zone"))

"""
F.broadcast sends the small zones table (265 rows) 
to every executor, so the 100M trips never 
move around the cluster. That’s what makes the join fast.
left keeps every trip even when a 
zone doesn’t match, so no trips disappear.
"""

obt = (trips
       .join(F.broadcast(pu), "PULocationID", "left")
       .join(F.broadcast(do), "DOLocationID", "left"))

"""
Turn codes into names
"""
VENDORS  = {1: "Creative Mobile Technologies", 2: "VeriFone", 4: "Unknown (4)"}
PAYMENTS = {1: "Credit card", 2: "Cash", 3: "No charge", 4: "Dispute", 5: "Unknown", 6: "Voided trip"}
RATES    = {1: "Standard rate", 2: "JFK", 3: "Newark", 4: "Nassau or Westchester",
            5: "Negotiated fare", 6: "Group ride", 99: "Unknown"}

obt = (obt
       .withColumn("vendor_name",  decode("VendorID", VENDORS))
       .withColumn("payment_desc", decode("payment_type", PAYMENTS))
       .withColumn("rate_desc",    decode("RatecodeID", RATES)))

"""
Add time columns and useful measures
"""
hour = F.hour("tpep_pickup_datetime")

obt = (obt
       .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
       .withColumn("pickup_hour", hour)
       .withColumn("pickup_weekday", F.date_format("tpep_pickup_datetime", "EEEE"))
       .withColumn("is_weekend", F.dayofweek("tpep_pickup_datetime").isin(1, 7))
       .withColumn("time_band",
                   F.when(hour.between(7, 9), "morning_rush")
                    .when(hour.between(10, 15), "midday")
                    .when(hour.between(16, 19), "evening_rush")
                    .when(hour.between(20, 23), "evening")
                    .otherwise("night"))
       .withColumn("avg_speed_mph",
                   F.when(F.col("trip_duration_min") > 0,
                          F.round(F.col("trip_distance") / (F.col("trip_duration_min") / 60), 2)))
       .withColumn("tip_pct",
                   F.when((F.col("payment_type") == 1) & (F.col("fare_amount") > 0),
                          F.round(F.col("tip_amount") / F.col("fare_amount") * 100, 2)))
       .withColumn("is_unknown_zone",
                   F.col("PULocationID").isin(264, 265) | F.col("DOLocationID").isin(264, 265)))

"""
Check the join is broadcast, then write to gold.obt partitioned by pickup year and month.
"""
obt.explain()   # look for "BroadcastHashJoin" twice — NOT "SortMergeJoin"

ensure_table(obt, "gold.obt_trips", ["pickup_year", "pickup_month"])
obt.writeTo("gold.obt_trips").overwritePartitions()

n_silver = trips.count()
n_gold   = spark.table("gold.obt_trips").count()
print(f"silver: {n_silver:,}  gold: {n_gold:,}")
print("ROW COUNT MATCHES ✅" if n_silver == n_gold else "MISMATCH ❌ (join duplicated or dropped rows)")

(spark.table("gold.obt_trips")
      .select(F.count_if(F.col("pu_zone").isNull()).alias("missing_pu_zone"),
              F.count_if(F.col("do_zone").isNull()).alias("missing_do_zone"))
      .show())

spark.stop()