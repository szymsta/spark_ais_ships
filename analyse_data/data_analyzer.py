from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col

class AnalyzeData:
    """
    The AnalyzeData class is responsible for analyzing a Spark DataFrame by filtering rows 
    based on the presence of message types specified in a predefined list.

    Attributes:
        spark_session (SparkSession): The Spark session to be used for DataFrame operations.
    
    Constants:
        DYNAMIC_MSG_TYPES (list): A list containing the valid message types to filter by.
        MESSAGE_TYPE (str): The column name for the message type.
    """

    DYNAMIC_MSG_TYPES : list[int] = [1, 2, 3, 18, 19]
    MESSAGE_TYPE = "msg_type"


    def __init__(self, spark_session: SparkSession):
        """
        Initializes the AnalyzeData object with a Spark session.

        Args:
            spark_session (SparkSession): An instance of SparkSession to perform data operations.
        """
        self.spark_session = spark_session
    

    def calculate_dynamic_data(self, df: DataFrame) -> DataFrame:
        """
        Filters the DataFrame to keep only rows with message types that are present in the
        predefined list of dynamic message types (DYNAMIC_MSG_TYPES).

        Args:
            df (DataFrame): The DataFrame containing the raw data to be analyzed.

        Returns:
            DataFrame: A filtered DataFrame containing only rows with valid message types.
        """
        return  df.filter(col(self.MESSAGE_TYPE).isin(self.DYNAMIC_MSG_TYPES))
