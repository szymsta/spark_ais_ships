from pyspark.sql import SparkSession, DataFrame


class LoadData:
    """
    The LoadData class is responsible for loading a dataset from a CSV file into a Spark DataFrame.
    It supports automatic schema inference and handling of headers.
    """
    FILE_NAME = "ais_decoded_message_time.csv"

    def __init__(self, spark_session: SparkSession):
        """
        Initializes the LoadData object.

        :param spark_session: An instance of SparkSession to handle data operations.
        """
        self.spark_session = spark_session
    

    def load_dataset(self) -> DataFrame:
        """
        Loads a dataset from a CSV file into a Spark DataFrame.

        The method applies the following options:
        - `header=True`: Treats the first row as column headers.
        - `inferSchema=True`: Automatically infers column data types.
        - `delimiter=","`: Uses a comma as the column separator.

        :return: A DataFrame containing the loaded dataset.
        """
        return(self.spark_session.read.format("csv")
                .options(header=True, inferSchema=True, delimiter=",")
                .load(self.FILE_NAME)
        )
