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
        ais_df = loader.join_datasets()

        # Clean the loaded data using the cleaner module
        ais_clean_df = cleaner.clean_dataset(ais_df)

        # Notify that data has been processed
    
    except Exception as e:
        # Handle errors during data loading or cleaning
        logging.error(f"Error loading or cleaning data: {e}")

        # Stop Spark session if modules fail to initialize
        spark.stop()    
        return
    

    # Analyze the data
    try:
        # Create a DataFrame with dynamic data
        dynamic_data_df = analyzer.calculate_dynamic_data(ais_clean_df)

        # Create a DataFrame grouped by the country
        country_df = analyzer.calculate_dynamic_data(ais_clean_df)

        # Create a DataFrame with distances
        distance_df = analyzer.calculate_distance(dynamic_data_df)

        # Create a DataFrame with speed average
        speed_avg_df = analyzer.calculate_avg_speed(dynamic_data_df)

    except Exception as e:
        # Handle errors during process
        logging.error(f"Error during data analysis: {e}")

        # Stop Spark session if modules fail to initialize
        spark.stop()    
        return


    # Search Data
    try:
        # Create a DataFrame by searching for MMSI (Maritime Mobile Service Identity)
        find_mmsi_df = searcher.search_ship(Config.TARGET_MMSI, dynamic_data_df)    # Use target from Config

        # Create a DataFrame by searching for a list of MMSIs from Config
        find_mmsi_list_df = searcher.search_ships(Config.TARGET_MMSI_LIST, dynamic_data_df) # Use target list from Config

        # Create a DataFrame by searching ships within a specific location range
        find_ships_by_location_df = searcher.search_ships_by_location(Config.LAT_MIN, Config.LAT_MAX, Config.LON_MIN, Config.LON_MAX , dynamic_data_df) # Use target list from Config

        # Create a DataFrame by searching ships by country flag
        find_ships_by_country_flag_df = searcher.search_ships_by_country_flag(Config.COUNTRY_FLAG, dynamic_data_df)

    except Exception as e:
        # Handle errors during process
        logging.error(f"Error during data search: {e}")

        # Stop Spark session if modules fail to initialize
        spark.stop()    
        return
    

    # Visualize data on a map
    try:
        # Create a clean DataFrame with ships data
        ships_map = visualizer.ships_map(find_ships_by_country_flag_df)

        # Save the map to an HTML file and open in default browser
        map_path = Config.MAP_OUTPUT_FILE   # Use name & path from Config
        ships_map.write_html(map_path)
        webbrowser.open(map_path)

        # Notify that map has been open in browser
        logging.info("The ships map has been successfully loaded and opened in the default browser")

    except Exception as e:
        # Handle errors during map visualization
        logging.error(f"Error during map visualization: {e}")

        # Stop Spark session if modules fail to initialize
        spark.stop()    
        return


if __name__ == "__main__":
    logging.info("Starting AIS data processing...")
    main()