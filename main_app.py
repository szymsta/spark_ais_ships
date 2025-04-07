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


# Initialize SparkSession (using Singleton)
spark = SparkSessionSingleton.get_spark_session()
logging.info("Spark session initialized.")


def main():

    # Initialize data processing modules
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


    # Load and clean datasets
    try:
        # Load the data using the loader module
        logging.info("Loading AIS datasets...")
        ais_df = loader.join_datasets()

        # Clean the loaded data using the cleaner module
        logging.info("Cleaning the dataset...")
        ais_clean_df = cleaner.clean_dataset(ais_df)

        # Notify that data has been processed
        logging.info("AIS data loaded and cleaned successfully.")
    
    except Exception as e:
        # Handle errors during data loading or cleaning
        logging.error(f"Error initializing modules: {e}")

        # Stop Spark session if modules fail to initialize
        spark.stop()    
        return
    

    # Analyze the data
    try:
        # Create a DataFrame with dynamic data
        logging.info("Filtering DataFrame...")
        dynamic_data_df = analyzer.calculate_dynamic_data(ais_clean_df)
        logging.info("Dynamic data DataFrame created")

        # Create a DataFrame grouped by the country
        logging.info("Filtering DataFrame...")
        country_df = analyzer.calculate_dynamic_data(ais_clean_df)
        logging.info("DataFrame grouped by country flag created")

        # Create a DataFrame with distances
        logging.info("Calculating the route distances for the ship...")
        distance_df = analyzer.calculate_distance(dynamic_data_df)
        logging.info("Ship's route distances DataFrame created")

        # Create a DataFrame with speed average
        logging.info("Calculating average speed for ships...")
        speed_avg_df = analyzer.calculate_avg_speed(dynamic_data_df)
        logging.info("DataFrame with speed average created")

    except Exception as e:
        # Handle errors during process
        logging.error(f"Error initializing modules: {e}")

        # Stop Spark session if modules fail to initialize
        spark.stop()    
        return


    # Search Data
    try:
        # Create a DataFrame by searching for MMSI (Maritime Mobile Service Identity)
        logging.info("Searching for MMSI in dynamic data...")
        find_mmsi_df = searcher.search_ship(319205600, dynamic_data_df)
        logging.info("DataFrame with provided MMSI number created")

        # Create a DataFrame by searching for a list of MMSIs
        logging.info("Searching for MMSI list in dynamic data...")
        find_mmsi_list_df = searcher.search_ships([319205600, 257988000], dynamic_data_df)
        logging.info("DataFrame with provided MMSI list created")

        # Create a DataFrame by searching ships within a specific location range
        logging.info("Searching ships located within specified geographic range...")
        find_ships_by_location_df = searcher.search_ships_by_location(57.0, 59.0, 4.0, 5.4, dynamic_data_df)
        logging.info("DataFrame with provided locations created")

        # Create a DataFrame by searching ships by country flag
        logging.info("Searching ships by country flag...")
        find_ships_by_country_flag_df = searcher.search_ships_by_country_flag("Poland (Republic of)", dynamic_data_df)
        logging.info("DataFrame with provided country created")

    except:
        # Handle errors during process
        logging.error(f"Error initializing modules: {e}")

        # Stop Spark session if modules fail to initialize
        spark.stop()    
        return
    

    # Visualize data on a map
    try:
        # Create a clean DataFrame with ships data
        logging.info("Loading map visualization of ships...")
        ships_map = visualizer.ships_map(find_ships_by_country_flag_df)

        # Save the map to an HTML file and open in default browser
        map_path = Config.MAP_OUTPUT_FILE   # Use name & path from Config
        ships_map.write_html(map_path)
        webbrowser.open(map_path)

        # Notify that map has been open in browser
        logging.info("The ship data map has been successfully loaded and opened in the default browser")

    except Exception as e:
        # Handle errors during map visualization
        logging.error(f"Error initializing modules: {e}")

        # Stop Spark session if modules fail to initialize
        spark.stop()    
        return


if __name__ == "__main__":
    logging.info("Starting AIS data processing...")
    main()