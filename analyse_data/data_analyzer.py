from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import col, first, udf, round
from pyspark.sql.types import DoubleType
from haversine import haversine, Unit


class AnalyzeData:
    """
    The AnalyzeData class is responsible for analyzing a Spark DataFrame by filtering rows 
    based on the presence of message types specified in a predefined list and calculating 
    distance between positions at different timestamps using the Haversine formula.

    Attributes:
        spark_session (SparkSession): The Spark session to be used for DataFrame operations.
    
    Constants:
        DYNAMIC_MSG_TYPES (list): A list containing the valid message types to filter by.
        MESSAGE_TYPE (str): The column name for the message type.
        COUNTRY (str): The column name for the country.
        MMSI (str): The column name for the Maritime Mobile Service Identity (unique ship identifier).
        TIMESTAMP (str): The column name for the timestamp.
        LATITUDE (str): The column name for latitude.
        LONGITUDE (str): The column name for longitude.
        TIMESTAMP_FOR_POS (str): The column name for the first timestamp used in distance calculation.
        SECOND_TIMESTAMP (str): The column name for the second timestamp used in distance calculation.
        SECOND_LAT (str): The column name for the second latitude used in distance calculation.
        SECOND_LON (str): The column name for the second longitude used in distance calculation.
        DISTANCE (str): The column name for the calculated distance between positions at different timestamps.
    """

    DYNAMIC_MSG_TYPES : list[int] = [1, 2, 3, 18, 19]
    MESSAGE_TYPE = "msg_type"
    COUNTRY = "country"
    MMSI = "mmsi"
    TIMESTAMP = "Timestamp"
    LATITUDE = "lat"
    LONGITUDE = "lon"
    TIMESTAMP_FOR_POS = "timestamp_for_pos"
    SECOND_TIMESTAMP = "second_timestamp"
    SECOND_LAT = "second_lat"
    SECOND_LON = "second_lon"
    DISTANCE = "distnce"


    # Define the static method for haversine UDF here
    @staticmethod
    @udf(DoubleType())
    def haversine_udf(lat1, lon1, lat2, lon2):
        """
        A user-defined function (UDF) to calculate the distance between two geographical points 
        using the Haversine formula.

        Args:
            lat1 (double): The latitude of the first point.
            lon1 (double): The longitude of the first point.
            lat2 (double): The latitude of the second point.
            lon2 (double): The longitude of the second point.

        Returns:
            double: The distance between the two points in kilometers.
        """
        return haversine((lat1, lon1), (lat2, lon2), unit=Unit.KILOMETERS)


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


    def calculate_country(self, df: DataFrame) -> DataFrame:
        """
        Groups the DataFrame by the country column, counts the number of unique ships 
        (identified by MMSI) for each country, and sorts the results in descending order 
        of the count of unique ships.

        Args:
            df (DataFrame): The DataFrame containing the data to be analyzed, which should include a 
                            column for country information and MMSI (Maritime Mobile Service Identity) 
                            to identify unique ships.

        Returns:
            DataFrame: A DataFrame with the countries grouped by their flag, showing the count of unique 
                    ships (MMSI), sorted in descending order by the count of unique ships.
        """
        return (df.dropDuplicates([self.MMSI])
                .groupBy(col(self.COUNTRY))
                .count()
                .sort("count", ascending=False)
        )
    
    def calculate_distance(self, df: DataFrame) -> DataFrame:
        """
        Calculates the distance between the first and last position of each ship (MMSI) based on the latitude
        and longitude recorded at the first and last timestamp.
        The distance is calculated using the Haversine formula.

        Args:
            df (DataFrame): The DataFrame containing the data to be analyzed, which should include 
                            the MMSI, latitude, longitude, and timestamp.

        Returns:
            DataFrame: A DataFrame containing the MMSI, first and second positions (latitude and longitude),
                       and the calculated distance between those positions in kilometers.
        """
        # Step 1: Create a window specification based on the partition column (MMSI), ordered by Timestamp
        window_asc = Window.partitionBy(self.MMSI).orderBy(col(self.TIMESTAMP))
        window_desc = Window.partitionBy(self.MMSI).orderBy(col(self.TIMESTAMP).desc())

        # Step 2: Get the first row in the group based on ascending timestamp
        df_1 = (df.withColumn(self.TIMESTAMP_FOR_POS, first(self.TIMESTAMP).over(window_asc))
                    .filter(col(self.TIMESTAMP) == col(self.TIMESTAMP_FOR_POS))
                    .select(self.MMSI, self.TIMESTAMP, self.LATITUDE, self.LONGITUDE, self.COUNTRY))

        # Step 3: Get the first row (which corresponds to the last position in time) in the group based on descending timestamp
        df_2 = (df.withColumn(self.TIMESTAMP_FOR_POS, first(self.TIMESTAMP).over(window_desc))
                    .filter(col(self.TIMESTAMP) == col(self.TIMESTAMP_FOR_POS))
                    .select(self.MMSI, col(self.TIMESTAMP).alias(self.SECOND_TIMESTAMP), col(self.LATITUDE).alias(self.SECOND_LAT), col(self.LONGITUDE).alias(self.SECOND_LON)))
        
        # Step 4: Join the first and last row based on MMSI
        dfs = df_1.join(df_2, self.MMSI, "inner")

        # Step 5: Add a new column calculating the distance between the positions using the Haversine formula
        dfs_distance = dfs.withColumn(self.DISTANCE, round(self.haversine_udf(
            col(self.LATITUDE),
            col(self.LONGITUDE),
            col(self.SECOND_LAT),
            col(self.SECOND_LON)
        ), 2)).orderBy(col(self.DISTANCE), ascending=False)

        # Step 6: Return the resulting DataFrame with the calculated distances
        return dfs_distance
