import sys
import logging
from haversine import Unit

class Config:
    """
    Configuration class for setting various parameters used across the application.

    This class contains all configuration values for the Spark environment, logging settings,
    file paths, data loader parameters, distance and speed conversion settings, and map visualization options.

    Constants:
        PYSPARK_PYTHON (str): Path to the Python interpreter used by PySpark.
        PYSPARK_DRIVER_PYTHON (str): Path to the Python interpreter used by the PySpark driver.
        SPARK_APP_NAME (str): The application name for the Spark session.
        SPARK_MASTER (str): The master URL for the Spark session.
        LOG_FILE (str): The log file name for logging.
        LOG_LEVEL (int): The log level to be used (e.g., INFO, DEBUG, ERROR).
        LOG_FORMAT (str): The format string for log messages.
        LOG_HANDLERS (list): The handlers for logging output (file and console).
        MAP_OUTPUT_FILE (str): The output file name and path for the map visualization.
        FILE_NAMES (list): List of CSV file names to load.
        FILE_FORMAT (str): The format of the files to be loaded.
        FILE_OPTIONS (dict): CSV loading options such as header, schema inference, and delimiter.
        DISTANCE_UNIT (Unit): The unit for measuring distance (e.g., kilometers).
        DYNAMIC_MSG_TYPES (list): List of message types associated with dynamic data.
        SPEED_CONVERSION_FACTOR (float): Conversion factor for speed from knots to kilometers per hour.
        MAP_ZOOM (int): Default zoom level for map visualization.
        MAP_HEIGHT (int): Default height for map visualization.
        MAP_STYLE (str): The style of the map visualization (e.g., "open-street-map").
    """

    # PySpark environment configuration - main_app.py
    PYSPARK_PYTHON = sys.executable         # Set the Python interpreter for PySpark
    PYSPARK_DRIVER_PYTHON = sys.executable  # Set the Python interpreter for the PySpark driver

    # Spark parameters - main_app.py
    SPARK_APP_NAME = "spark_ais_ships"  # Set the application name for the Spark session
    SPARK_MASTER = "local[*]"           # Set the master URL to run Spark locally with all available cores

    # Logging settings - main_app.py
    LOG_FILE = "spark_process.log"  # Specify the log file name
    LOG_LEVEL = logging.INFO        # Set the log level to INFO (other options could be DEBUG, WARNING, ERROR, etc.)
    LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s" # Log format string
    LOG_HANDLERS = [logging.FileHandler(LOG_FILE), logging.StreamHandler()] # Default handlers: to a file and to the console

    # map name & path - main_app.py
    MAP_OUTPUT_FILE = "ships_map.html"


    # File name configuration - data_loader.py
    FILE_NAMES = ["ais_decoded_message_time.csv", "mid_for_mmsi.csv"]

    # FIle format - data_loader.py
    FILE_FORMAT = "csv"

    # CSV loading options - data_loader.py
    FILE_OPTIONS = {
        "header": True,
        "inferSchema": True,
        "delimiter": ","
    }


    # Distance unit - data_analyzer.py
    DISTANCE_UNIT = Unit.KILOMETERS

    # Dynamic data - data_analyzer.py
    DYNAMIC_MSG_TYPES : list[int] = [1, 2, 3, 18, 19]   # msg_type with dynamic data

    # Speed conversion factor - data_analyzer.py
    SPEED_CONVERSION_FACTOR = 1.852  # Conversion factor for speed from knots to km/h: 1 knot = 1.852 km/h


    # Map visualization parameters - data_visualization.py
    MAP_ZOOM = 5                    # Default zoom level for the map
    MAP_HEIGHT = 5000               # Default map height
    MAP_STYLE = "open-street-map"   # Default map style
