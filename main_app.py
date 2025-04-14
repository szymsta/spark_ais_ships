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

        # Usage 1 - Create a DataFrame grouped by the country
        country_df = analyzer.calculate_country(dynamic_data_df)

        # Usage 2 - Create a DataFrame with distances
        distance_df = analyzer.calculate_distance(dynamic_data_df)

        # Usage 3 - Create a DataFrame with speed average
        speed_avg_df = analyzer.calculate_avg_speed(dynamic_data_df)

    except Exception as e:
        # Handle errors during process
        logging.error(f"Error during data analysis: {e}")

        # Stop Spark session if modules fail to initialize
        spark.stop()    
        return


    # Usage 4 - Create a map with the route for the selected MMSI 
    try:
        # Step 1: Create DataFrame with the selected MMSI
        find_mmsi_df = searcher.search_ship(Config.TARGET_MMSI, dynamic_data_df)    # Use target from Config

        # Step 2: Create a ship route map
        ship_map_1 = visualizer.ships_map(find_mmsi_df)

        # Step 3: Assign the output file path to the variable
        map_path_1 = Config.MAP_OUTPUT_FILE_1   # Use the file name & path defined in Config

        # Step 4: Save the map as an HTML file
        ship_map_1.write_html(map_path_1)

        # Step 5: Open the saved map in the default web browser
        webbrowser.open(map_path_1)

        # Log the result
        logging.info("The map has been generated and opened in the default browser")

    except Exception as e:
        # Handle errors during process
        logging.error(f"Error during map generation: {e}")

        # Stop Spark session if modules fail to initialize
        spark.stop()    
        return
    

    # Usage 5 - Create a map with the route for the selected location range and country flag
    try:

        # Step 1: Create DataFrame with the selected location and country flag
        find_ships_by_country_flag_df = searcher.search_ships_by_location(
                                                Config.LAT_MIN, Config.LAT_MAX,     # Use lat range from Config
                                                Config.LON_MIN, Config.LON_MAX,     # Use lon range from Config
                                                (searcher.search_ships_by_country_flag(Config.COUNTRY_FLAG, dynamic_data_df))) # Use country flag from Config
        
        # Step 2: Create a ship route map
        ships_map_2 = visualizer.ships_map(find_ships_by_country_flag_df)

        # Step 3: Assign the output file path to the variable
        map_path_2 = Config.MAP_OUTPUT_FILE_2   # Use the file name & path defined in Config

        # Step 4: Save the map as an HTML file
        ships_map_2.write_html(map_path_2)

        # Step 5: Open the saved map in the default web browser
        webbrowser.open(map_path_2)

        # Log the result
        logging.info("The map has been generated and opened in the default browser")

    except Exception as e:
        # Handle errors during process
        logging.error(f"Error during map generation: {e}")

        # Stop Spark session if modules fail to initialize
        spark.stop()    
        return


    # Usage 6 - Create a map with the route for selected MMSIs
    try:
        # Step 1: Create a DataFrame by searching ships by country flag
        find_selected_mmsi_df = searcher.search_ships(Config.TARGET_MMSI_LIST, dynamic_data_df) # Use mmsi list from Config

        # Step 2: Create a ship route map
        ships_map_3 = visualizer.ships_map(find_selected_mmsi_df)

        # Step 3: Assign the output file path to the variable
        map_path_3 = Config.MAP_OUTPUT_FILE_3   # Use the file name & path defined in Config

        # Step 4: Save the map as an HTML file
        ships_map_3.write_html(map_path_3)

        # Step 5: Open the saved map in the default web browser
        webbrowser.open(map_path_3)

        # Log the result
        logging.info("The map has been generated and opened in the default browser")

    except Exception as e:
        # Handle errors during map visualization
        logging.error(f"Error during map visualization: {e}")

        # Stop Spark session if modules fail to initialize
        spark.stop()    
        return


if __name__ == "__main__":
    logging.info("Starting AIS data processing...")
    main()