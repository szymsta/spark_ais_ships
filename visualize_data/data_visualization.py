from pyspark.sql import SparkSession, DataFrame
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd


class VisualizeData:
    """
    This class uses Plotly to create interactive maps and needs a SparkSession to handle large datasets.
    The `ships_map` method shows the locations of ships based on their latitude, longitude, and MMSI (Maritime Mobile Service Identity) numbers.
    """

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
        ships_map = df.select("mmsi", "lon", "lat").toPandas()

        # Step 2: Create the map using Plotly Express and layout with OpenStreetMap style
        ships_map = (px.scatter_mapbox(ships_map,
                                    lat="lat",
                                    lon="lon",
                                    hover_name="mmsi",
                                    zoom=5,
                                    height=5000)
                                    .update_layout(mapbox_style="open-street-map")
        )

        # Step 3: Return the map (go.Figure)
        return ships_map
    

