from pyspark.sql import SparkSession

# Initialize SparkSession.
spark = (
    SparkSession.builder
    .appName("spark_twtr_pipeline")     # Set the application name
    .master("local[*]")                 # Run Spark locally with as many worker threads as there are cores on your machine
    .getOrCreate()                      # Get or create a Spark session
    )