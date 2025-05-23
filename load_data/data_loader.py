from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import substring, col
from functools import reduce
from config import Config


class LoadData:
    """
    The LoadData class is responsible for loading a dataset from a CSV file into a Spark DataFrame.
    It supports automatic schema inference and handling of headers.

    The class provides functionality to:
    - Load a dataset from a CSV file with predefined options (header, schema inference, delimiter).
    - Extract the Maritime Identification Digits (MID) from the MMSI column if present.
    - Merge multiple datasets using a left join on the MID column.

    Attributes:
        spark_session (SparkSession): The Spark session used for DataFrame operations.

    Constants:
        MMSI (str): The column name for the Maritime Mobile Service Identity.
        MID (str): The column name for the Maritime Identification Digits.
    
    Configuration:
        COnfig.FILE_NAMES (list): The list of CSV file names to be loaded, defined in the config.py file.
        Config.FILE_FORMAT (str): File format used when reading data (e.g., "csv").
        Config.FILE_OPTIONS (dict): Dictionary of Spark CSV read options (e.g., header, delimiter).
    """
    
    MMSI = "mmsi"
    MID = "mid"

    def __init__(self, spark_session: SparkSession):
        """
        Initializes the LoadData class with a Spark session.

        Args:
            spark_session (SparkSession): An instance of SparkSession to perform data operations.
        """
        self.spark_session = spark_session
    

    def load_datasets(self, file_name: str) -> DataFrame:
        """
        Loads a single dataset from a CSV file with predefined options.

        Args:
            file_name (str): The name of the CSV file to load.

        Returns:
            DataFrame: A DataFrame containing the loaded dataset.
        """
        # Step 1: Create Data Frame
        df = (self.spark_session.read.format(Config.FILE_FORMAT)    # Use format from Config
              .options(**Config.FILE_OPTIONS)   # Use options from Config
              .load(file_name))
        
        # Step 2: Extract MID from MMSI if the column exists
        if self.MMSI in df.columns:
            df = df.withColumn(self.MID, substring(col(self.MMSI), 1, 3).cast("int"))
        
        # Step 3: Return df
        return df

    

    def join_datasets(self) -> DataFrame:
        """
        Loads and joins all datasets on the MID column using a left join.

        Returns:
            DataFrame: A merged DataFrame containing all datasets.
        """
        dfs = [self.load_datasets(file) for file in Config.FILE_NAMES]  # Use file names from config
        return reduce(lambda df1, df2: df1.join(df2, self.MID, "left"), dfs)
