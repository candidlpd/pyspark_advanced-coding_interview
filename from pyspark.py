from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder.appName("PySparkTest").getOrCreate()

# Print Spark session details
print(spark)
