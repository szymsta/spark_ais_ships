# Import libraries
from pyspark.sql import SparkSession
import logging
import webbrowser

# Import internal modules
from load_data.data_loader import LoadData
from clear_data.data_cleaner import CleanData
from visualize_data.data_visualization import VisualizeData


# Configure logging
logging.basicConfig(
    level=logging.INFO,    # Set the logging level to INFO 
    format='%(asctime)s - %(levelname)s - %(message)s', # Define the format for log messages
    handlers=[logging.FileHandler("spark_process.log"), logging.StreamHandler()]  # Set up two handlers: to a file and to the console
)


def main():

    # Initialize SparkSession.
    spark = (
        SparkSession.builder
        .appName("spark_ais_ships")         # Set the application name
        .master("local[*]")                 # Run Spark locally with as many worker threads as there are cores on your machine
        .getOrCreate()                      # Get or create a Spark session
        )

    logging.info("Spark session initialized.")


    # Initialize modules
    try:
        loader = LoadData(spark)
        cleaner = CleanData(spark)
        visualizer = VisualizeData(spark)
    
    except Exception as e:
        # Handle errors during data loading or cleaning
        logging.error(f"Error initializing modules: {e}")

        # Stop Spark session if modules fail to initialize
        spark.stop()                                
        return


    # Load and clean data
    try:
        logging.info("Loading data...")
        # Load the data using the loader module and cache it for better performance
        ais_df = loader.load_dataset()

        logging.info("Cleaning data...")
        # Clean the loaded data using the cleaner module
        ais_clean_df = cleaner.clean_dataset(ais_df)

        # Notify that data has been processed
        logging.info("Data loaded and cleaned successfully.")
    
    except Exception as e:
        # Handle errors during data loading or cleaning
        logging.error(f"Error initializing modules: {e}")

        # Stop Spark session if modules fail to initialize
        spark.stop()    
        return
    

    # Visualize data on map
    try:
        logging.info("Loading map...")
        # Create clean df with ships data
        ships_map = visualizer.ships_map(ais_clean_df)

        # Save the map to an HTML file
        map_path = "ships_map.html"
        ships_map.write_html(map_path)
        
        # Open map in default browser
        webbrowser.open(map_path)

        # Notify that map has been open in browser
        logging.info("The ship map has been successfully loaded and opened in browser")

    except Exception as e:
        # Handle errors during map visualization
        logging.error(f"Error initializing modules: {e}")

        # Stop Spark session if modules fail to initialize
        spark.stop()    
        return


if __name__ == "__main__":
    logging.info("Starting AIS data processing...")
    main()