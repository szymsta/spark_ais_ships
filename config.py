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
        TARGET_MMSI (int): A single target MMSI number used for filtering specific ships.
        TARGET_MMSI_LIST (list): A list of MMSI numbers used to search for multiple specific ships.
        LAT_MIN (float): Minimum latitude value for location-based filtering.
        LAT_MAX (float): Maximum latitude value for location-based filtering.
        LON_MIN (float): Minimum longitude value for location-based filtering.
        LON_MAX (float): Maximum longitude value for location-based filtering.
        COUNTRY_FLAG (str): Country name used to filter ships by their registration flag.
        MAP_OUTPUT_FILE_1 (str): Output file path for the single ship map visualization.
        MAP_OUTPUT_FILE_2 (str): Output file path for the country-flag ship map visualization.
        MAP_OUTPUT_FILE_3 (str): Output file path for the multiple selected ships map visualization.
        FILE_NAMES (list): List of CSV file names to load.
        FILE_FORMAT (str): The format of the files to be loaded.
        FILE_OPTIONS (dict): CSV loading options such as header, schema inference, and delimiter.
        DISTANCE_UNIT (Unit): The unit for measuring distance (e.g., kilometers).
        DYNAMIC_MSG_TYPES (list): List of message types associated with dynamic AIS data.
        SPEED_CONVERSION_FACTOR (float): Conversion factor for speed from knots to kilometers per hour.
        MMSI_VALID_PREFIX_PATTERN (str): Pattern for valid ship MMSI numbers starting with digits 2–7.
        MAP_ZOOM (int): Default zoom level for map visualization.
        MAP_HEIGHT (int): Default height for map visualization in pixels.
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
    
    # Search data parameters:
    TARGET_MMSI = 477327900                                         # MMSI number for searching a single ship
    TARGET_MMSI_LIST = [477327900, 371861000, 247229700, 309075000,
                        355931000, 256281000, 249652000]            # List of MMSI numbers for searching multiple ships
    LAT_MIN, LAT_MAX = 56.0, 61.0                                   # Latitude range for searching ships by location
    LON_MIN, LON_MAX = 9.0, 11.0                                    # Longitude range for searching ships by location
    COUNTRY_FLAG = "Norway"                                         # Country flag for searching ships by their country of registration

    # map name & path - main_app.py
    MAP_OUTPUT_FILE_1 = "route_single_ship_map.html"
    MAP_OUTPUT_FILE_2 = "route_ships_country_map.html"
    MAP_OUTPUT_FILE_3 = "route_selected_ships_map.html"


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

    # MMSI validation pattern - data_analyzer.py
    MMSI_VALID_PREFIX_PATTERN = "^[2-7]"    # valid ship MMSI numbers starting with digits 2–7


    # Map visualization parameters - data_visualization.py
    MAP_ZOOM = 5                    # Default zoom level for the map
    MAP_HEIGHT = 5000               # Default map height
    MAP_STYLE = "open-street-map"   # Default map style
