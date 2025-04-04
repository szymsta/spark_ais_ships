from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from functools import reduce


class SearchData:
    """
    The SearchData class is responsible for performing search operations on a Spark DataFrame, specifically 
    searching for ships based on their Maritime Mobile Service Identity (MMSI).
    It includes methods for:
        - Searching for ships based on their MMSI.
        - Searching for ships within a list of MMSIs.
        - Filtering ships within a specified geographical location range (latitude and longitude).


    Attributes:
        spark_session (SparkSession): The Spark session to be used for DataFrame operations.
    
    Constants:
        MMSI (str): The column name for the Maritime Mobile Service Identity (MMSI), which is a unique identifier for ships.
        LATITUDE (str): The column name for latitude in the DataFrame.
        LONGITUDE (str): The column name for longitude in the DataFrame.
    """

    MMSI = "mmsi"
    LATITUDE = "lat"
    LONGITUDE = "lon"

    def __init__(self, spark_session: SparkSession):
        """
        Initializes the SearchData object with a Spark session.

        Args:
            spark_session (SparkSession): An instance of SparkSession to perform data operations.
        """
        self.spark_session: spark_session


    def search_ship(self, mmsi: int,  df: DataFrame) -> DataFrame:
        """
        Searches the DataFrame for rows containing the specified MMSI. This method filters the DataFrame based on 
        the presence of the given MMSI.

        Args:
            mmsi (int): The Maritime Mobile Service Identity (MMSI) of the ship to search for.
            df (DataFrame): The DataFrame containing the ship data to be searched. It must include a column named `mmsi`.

        Returns:
            DataFrame: A DataFrame containing the rows where the MMSI matches the provided value.
        """
        return df.filter(col(self.MMSI).contains(mmsi))
    

    def search_ships(self, mmsi_list: list, df: DataFrame) -> DataFrame:
        """
        Searches the DataFrame for rows containing any of the specified MMSIs from the provided list. 
        This method filters the DataFrame using a logical OR operation between the different MMSIs in the list.

        Args:
            mmsi_list (list): A list of Maritime Mobile Service Identity (MMSI) values to search for.
            df (DataFrame): The DataFrame containing the ship data to be searched. It must include a column named `mmsi`.

        Returns:
            DataFrame: A DataFrame containing the rows where the `mmsi` column matches any of the MMSIs from the provided list.
        """
        # Step 1: Generate search criteria for each MMSI in the list
        search_criteria = [(col(self.MMSI).isNotNull()).contains(mmsi) for mmsi in mmsi_list]

        # Step 2: Apply logical OR to all search criteria, filter and return the DataFrame
        return (df.filter(reduce(lambda x, y: x | y, search_criteria)))
    

    def search_ships_by_location(self,
        lat_min: float,
        lat_max: float,
        lon_min: float,
        lon_max: float,
        df: DataFrame
    ) -> DataFrame:
        """
        Filters the DataFrame for ships within the specified geographical location.
        This method filters the input DataFrame to include only the rows where the ship's 
        latitude and longitude fall within the specified minimum and maximum latitude 
        and longitude values.

        Args:
            lat_min (float): The minimum latitude value for the geographical range.
            lat_max (float): The maximum latitude value for the geographical range.
            lon_min (float): The minimum longitude value for the geographical range.
            lon_max (float): The maximum longitude value for the geographical range.
            df (DataFrame): The DataFrame containing the ship data, which must include columns 
                            for latitude and longitude.

        Returns:
            DataFrame: A DataFrame filtered to include only ships whose latitude and longitude 
                    are within the specified bounds.
        """
        return df.filter((col(self.LATITUDE).between(lat_min, lat_max)) &
                        (col(self.LONGITUDE).between(lon_min, lon_max)))




