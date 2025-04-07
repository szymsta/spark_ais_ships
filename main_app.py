# Import libraries
import logging
import webbrowser
import os

# Import internal modules
from load_data.data_loader import LoadData
from clean_data.data_cleaner import CleanData
from visualize_data.data_visualization import VisualizeData
from analyse_data.data_analyzer import AnalyzeData
from search_data.data_searcher import SearchData
from spark_session_manager import SparkSessionSingleton
from config import Config


# Set the same Python interpreter for both PySpark and the driver to avoid compatibility issues.
os.environ['PYSPARK_PYTHON'] = Config.PYSPARK_PYTHON
os.environ['PYSPARK_DRIVER_PYTHON'] = Config.PYSPARK_DRIVER_PYTHON


# Configure logging
logging.basicConfig(
    level = Config.LOG_LEVEL,       # Logging threshold (e.g., INFO, DEBUG)
    format = Config.LOG_FORMAT,     # Format for each log message
    handlers = Config.LOG_HANDLERS  # Output destinations: log file and console
)


# Initialize SparkSession outside of main function (using Singleton)
spark = SparkSessionSingleton.get_spark_session()
logging.info("Spark session initialized.")


def main():

    # Initialize modules
    try:
        loader = LoadData(spark)
        cleaner = CleanData(spark)
        visualizer = VisualizeData(spark)
        analyzer = AnalyzeData(spark)
        searcher = SearchData(spark)
    
    except Exception as e:
        # Handle errors during data loading or cleaning
        logging.error(f"Error initializing modules: {e}")

        # Stop Spark session if modules fail to initialize
        spark.stop()                                
        return


    # Load and clean data
    try:
        # Load the data using the loader module
        logging.info("Loading data...")
        ais_df = loader.join_datasets()

        # Clean the loaded data using the cleaner module
        logging.info("Cleaning data...")
        ais_clean_df = cleaner.clean_dataset(ais_df)

        # Notify that data has been processed
        logging.info("Data loaded and cleaned successfully.")
    
    except Exception as e:
        # Handle errors during data loading or cleaning
        logging.error(f"Error initializing modules: {e}")

        # Stop Spark session if modules fail to initialize
        spark.stop()    
        return
    

    # Analyze data
    try:
        # Create a DataFrame with dynamic data
        logging.info("Filtering df...")
        dynamic_data = analyzer.calculate_dynamic_data(ais_clean_df)
        logging.info("DataFrame with dynamic data created")

        # Create a DataFrame with distances
        logging.info("Calculating distances...")
        distance = analyzer.calculate_distance(dynamic_data)
        logging.info("DataFrame with distances created")

        # Create a DataFrame with speed average
        logging.info("Calculating speed average...")
        speed_avg = analyzer.calculate_avg_speed(dynamic_data)
        logging.info("DataFrame with speed average created")

    except Exception as e:
        # Handle errors during process
        logging.error(f"Error initializing modules: {e}")

        # Stop Spark session if modules fail to initialize
        spark.stop()    
        return


    # Search Data
    try:
        # Create a DataFrame with search MMSI
        logging.info("Calculating df...")
        find_mmsi = searcher.search_ship(319205600, dynamic_data)
        logging.info("DataFrame with provided key_word created")

        # Create a DataFrame with searching by location
        logging.info("Calculating df...")
        find_ships_by_location = searcher.search_ships_by_location(57.0, 59.0, 4.0, 5.4, dynamic_data)
        logging.info("DataFrame with provided locations created")

    except:
        # Handle errors during process
        logging.error(f"Error initializing modules: {e}")

        # Stop Spark session if modules fail to initialize
        spark.stop()    
        return
    

    # Visualize data on map
    try:
        # Create clean df with ships data
        logging.info("Loading map...")
        ships_map = visualizer.ships_map(find_ships_by_location)

        # Save the map to an HTML file and open in default browser
        map_path = Config.MAP_OUTPUT_FILE   # Use name & path from Config
        ships_map.write_html(map_path)
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