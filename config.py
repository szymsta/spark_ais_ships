import sys
import logging
from haversine import Unit

class Config:

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
