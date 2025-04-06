from pyspark.sql import SparkSession, DataFrame
import plotly.express as px
import plotly.graph_objects as go
from config import Config


class VisualizeData:
    """
    This class uses Plotly to create interactive maps and needs a SparkSession to handle large datasets.
    The `ships_map` method shows the locations of ships based on their latitude, longitude, and MMSI (Maritime Mobile Service Identity) numbers.

    Attributes:
    LATITUDE (str): The column name for the latitude of the ships.
    LONGITUDE (str): The column name for the longitude of the ships.
    MMSI (str): The column name for the Maritime Mobile Service Identity of the ships.
    """

    LATITUDE = "lat"
    LONGITUDE = "lon"
    MMSI = "mmsi"

    def __init__(self, spark_session: SparkSession):
        """
        Initializes the VisualizeData object with a Spark session.

        Args:
            spark_session (SparkSession): An instance of SparkSession to perform data operations.
        """
        self.spark_session = spark_session


    def ships_map(self, df: DataFrame) -> go.Figure:
        """
        Creates a map visualization showing the locations of ships using Plotly's scatter_mapbox.

        This method converts a Spark DataFrame into a Pandas DataFrame and uses Plotly Express to generate an interactive
        map that visualizes ship locations based on their latitude, longitude, and MMSI (Maritime Mobile Service Identity).

        Args:
            df (DataFrame): A PySpark DataFrame containing ship data, including 'lat' (latitude), 'lon' (longitude), 
                            and 'mmsi' (Maritime Mobile Service Identity) columns.

        Returns:
            go.Figure: A Plotly `go.Figure` object representing the ship location map.
        """
        # Step 1: Convert Spark DataFrame to Pandas DataFrame
        ships_map = df.select(self.MMSI, self.LONGITUDE, self.LATITUDE).toPandas()

        # Step 2: Create the map using Plotly Express and layout with OpenStreetMap style
        ships_map = (px.scatter_mapbox(ships_map,
                                    lat=self.LATITUDE,
                                    lon=self.LONGITUDE,
                                    hover_name=self.MMSI,
                                    zoom=Config.MAP_ZOOM,   # Use zoom from Config
                                    height=Config.MAP_HEIGHT)   # Use height from Config
                                    .update_layout(mapbox_style=Config.MAP_STYLE) # Use mapbox style from Config
        )

        # Step 3: Return the map (go.Figure)
        return ships_map
    