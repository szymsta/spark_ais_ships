from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import DoubleType, IntegerType, TimestampType

class CleanData:
    """
    The CleanData class is responsible for cleaning a Spark DataFrame by performing the following transformations:
    - Casting specified columns to predefined types.
    - Converting a specified Timestamp column to the correct `TimestampType` format.
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

        Args:
            df (DataFrame): The DataFrame containing the raw data that needs to be cleaned.

        Returns:
            DataFrame: A cleaned DataFrame with transformed column types and timestamp format.
        """

        # List of transformations based on predefined column types
        change_schema = [
            col(col_name).cast(self.column_types[col_name]) if col_name in self.column_types
            else col(col_name)
            for col_name in df.columns
        ]

        # Convert the 'Timestamp' column to TimestampType if it exists in the DataFrame
        if self.timestamp_column in df.columns:
            df = df.withColumn(self.timestamp_column, to_timestamp(col(self.timestamp_column), self.timestamp_format))

        # Return the DataFrame with the updated schema
        return df.select(*change_schema)
