import logging

from pyspark.sql import SparkSession

# Set log level to ERROR
logging.getLogger("py4j").setLevel(logging.ERROR)
# Create a Spark session
spark = SparkSession.builder.appName("SparkTest").master("spark://spark-master:7077").getOrCreate()

# Create a DataFrame
data = [("Alice", 25), ("Bob", 30), ("Carol", 22)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

# Show the DataFrame
df.show()

# Stop the Spark session
spark.stop()
