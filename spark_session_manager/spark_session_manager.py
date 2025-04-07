from pyspark.sql import SparkSession
from config import Config


class SparkSessionSingleton:
    """
    A class managing the SparkSession instance as a singleton.
    Ensures that the SparkSession is created only once and is available throughout the application.

    Constants:
        _spark_session (SparkSession): The single instance of the SparkSession that is shared across the application.
    """

    # Variable with SparkSession instance (initially set to None)
    _spark_session = None


    @staticmethod
    def get_spark_session():
        """
        Static method to get the SparkSession instance.
        If the session has not been created yet, it is created and stored.

        Returns:
            SparkSession: The SparkSession instance, either newly created or retrieved from the singleton.
        """

        # Check if the SparkSession has been created, if not create it
        if SparkSessionSingleton._spark_session is None:
            SparkSessionSingleton._spark_session = (
                SparkSession.builder
                    .appName(Config.SPARK_APP_NAME) # Use app name from Config
                    .master(Config.SPARK_MASTER)    # Use master from Config
                    .getOrCreate()
            )

        return SparkSessionSingleton._spark_session
