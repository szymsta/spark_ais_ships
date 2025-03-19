# # Import libraries
from pyspark.sql import SparkSession

# Import internal modules
from load_data.data_loader import LoadData


def main():

    # Initialize SparkSession.
    spark = (
        SparkSession.builder
        .appName("spark_ais_ships")         # Set the application name
        .master("local[*]")                 # Run Spark locally with as many worker threads as there are cores on your machine
        .getOrCreate()                      # Get or create a Spark session
        )

    print("Spark session initialized.")


    # Initialize modules
    try:
        loader = LoadData(spark)
    
    except Exception as e:
        # Handle errors during data loading or cleaning
        print(f"Error initializing modules: {e}")

        # Stop Spark session if modules fail to initialize
        spark.stop()                                
        return


    # Load data
    try:
        print("Loading data...")
        ais_df = loader.load_dataset()
        ais_df.show()
    
    except Exception as e:
        # Handle errors during data loading or cleaning
        print(f"Error during data loading/cleaning: {e}")

        # Stop Spark session if modules fail to initialize
        spark.stop()    
        return


if __name__ == "__main__":
    main()