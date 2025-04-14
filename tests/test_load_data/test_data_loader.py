import pytest
from load_data.data_loader import LoadData
from tests.spark_session_test import spark
from pyspark.sql.functions import length


# Prepare function as fixture
@pytest.fixture
def raw_data(spark):
    """
    Fixture to load raw data from the 'test_file.csv' using LoadData class.

    Returns:
        DataFrame: Raw data loaded as a Spark DataFrame.
    """
    loader = LoadData(spark)
    return loader.load_datasets("test_file.csv")


# Test spark session
def test_spark_session_is_created_and_valid(spark):
    """
    Test that the Spark session is successfully created.

    Verifies:
        - The Spark session fixture returns a non-None object.
        - The Spark session has a valid version attribute set.
    """
    assert spark is not None
    assert spark.version is not None


# Test load function
def test_create_mid_column_with_valid_values(spark, raw_data):
    """
    This test ensures that the 'mid' column is properly generated from the MMSI column.

    Args:
        spark (SparkSession): The Spark session used to run the test.

    Verifies:
        - 'mid' column exists in the loaded DataFrame.
        - 15 rows are loaded from the CSV file.
        - one row with 'mid' equal to 232.
        - 'mid' values have exactly 3 characters.
    """
    # Create DF
    ais_df = raw_data

    # Test 1: Check if 'mid' column exists and correct number of rows is loaded
    assert "mid" in ais_df.columns  # The 'mid' column should exist
    assert ais_df.count() == 15   # Exactly 15 rows should be loaded

    # Test 2: Check for at least one row with 'mid' equal to 232
    assert ais_df.filter(ais_df["mid"] == 232).count() >= 1 # At least one match expected
    
    # Test 3: Ensure all 'mid' values have exactly 3 characters
    mid_length_count = ais_df.filter((length(ais_df["mid"].cast("string")) != 3)).count()
    assert mid_length_count == 0    # Should be 0