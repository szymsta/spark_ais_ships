from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import DoubleType, IntegerType, TimestampType

class CleanData:
    """
    The CleanData class is responsible for cleaning a Spark DataFrame by performing the following transformations:
    - Casting specified columns to predefined types.
    - Converting a specified Timestamp column to the correct `TimestampType` format.
    - Filtering rows based on the validity of geographic coordinates (longitude and latitude), speed, course, and presence of the message type.
    
     Attributes:
        spark_session (SparkSession): The Spark session to be used for DataFrame operations.
        column_types (dict): A dictionary mapping column names to their desired data types.
        timestamp_column (str): The name of the column containing timestamp data.
        timestamp_format (str): The format string for parsing the timestamp column.
    """

    def __init__(self, spark_session: SparkSession):
        """
        Initializes the CleanData object with a Spark session.

        Args:
            spark_session (SparkSession): An instance of SparkSession to perform data operations.
        """
        self.spark_session = spark_session

        # Dictionary specifying the desired column types for transformation
        self.column_types = {
            "msg_type": IntegerType(),
            "mmsi": IntegerType(),
            "lon": DoubleType(),
            "lat": DoubleType(),
            "speed": DoubleType(),
            "course": DoubleType(),
        }

        # Column and format for timestamp conversion
        self.timestamp_column = "Timestamp"
        self.timestamp_format = "dd.MM.yyyy HH:mm:ss.SSS"
    

    def clean_dataset(self, df: DataFrame) -> DataFrame:
        """
        Cleans the provided DataFrame by transforming columns according to predefined types and
        converting a timestamp column to the correct format.

        The method performs the following transformations:
        - Casts columns to their appropriate data types as defined in the `column_types` dictionary.
        - Converts the `Timestamp` column (if it exists) to a `TimestampType` using the specified format.
        - Filters out rows where:
            - Message Type (`msg_type`) is null.
            - Longitude (`lon`) is outside the valid range of -180 to 180.
            - Latitude (`lat`) is outside the valid range of -90 to 90.
            - Speed (`speed`) is outside the valid range of 0 to 102.2.
            - Course (`course`) is outside the valid range of 0 to 360.

        Args:
            df (DataFrame): The DataFrame containing the raw data that needs to be cleaned.

        Returns:
            DataFrame: A cleaned DataFrame with transformed column types and timestamp format.
        """

        # Step1: Create list of transformations based on predefined column types
        change_schema = [
            col(col_name).cast(self.column_types[col_name]) if col_name in self.column_types
            else col(col_name)
            for col_name in df.columns
        ]

        # Step 2: Convert the 'Timestamp' column to TimestampType if it exists in the DataFrame
        if self.timestamp_column in df.columns:
            df = df.withColumn(self.timestamp_column, to_timestamp(col(self.timestamp_column), self.timestamp_format))
        
        # Step 3: Update the DataFrame with the transformed schema
        df = df.select(*change_schema)

        # Step 4: Apply multiple filters in one step for better performance
        df = df.filter(
            (col("msg_type").isNotNull()) &                 # Remove rows without msg_type
            (col("lon") >= -180) & (col("lon") <= 180) &    # Valid longitude range
            (col("lat") >= -90) & (col("lat") <= 90) &      # Valid latitude range
            (col("speed") >= 0) & (col("speed") < 102.2) &  # Valid speed range (0 ≤ speed < 102.2)
            (col("course") >= 0) & (col("course") <= 360)   # Valid course range (0 ≤ course ≤ 360)
        )

        # Step 5: Return the cleaned and transformed DataFrame
        return df
