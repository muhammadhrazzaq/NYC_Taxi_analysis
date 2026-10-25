from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("smoke-test").getOrCreate()

spark.sql("SHOW NAMESPACES").show()
spark.sql("""
    CREATE TABLE IF NOT EXISTS bronze.smoke_test (id INT, name STRING)
    USING iceberg
""")
spark.sql("INSERT INTO bronze.smoke_test VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')")
spark.sql("SELECT * FROM bronze.smoke_test").show()

spark.stop()