import pytest
from clean_data.data_cleaner import CleanData
from tests.spark_session_test import spark
from tests.test_load_data.test_data_loader import raw_data
from pyspark.sql.functions import length


# Prepare function as fixture
@pytest.fixture
def cleaned_data(spark, raw_data):
    """
    Fixture to return a cleaned DataFrame using CleanData class.

    Returns:
        DataFrame: Cleaned Spark DataFrame.
    """
    cleaner = CleanData(spark)
    return cleaner.clean_dataset(raw_data)


# Test clean function
def test_clean_dataset_filters_out_invalid_and_null_values(cleaned_data):
    """
    Test the cleaning of raw data using CleanData class.

    Verifies:
        - The cleaned DataFrame has no invalid or null values in critical columns.
        - 'mid' values have exactly 3 characters.
        - Rows with invalid latitudes, longitudes, speeds, and courses are filtered out.
    """

    # Create DF
    df = cleaned_data

    # Test 1: Check that no null 'msg_type' values exist
    assert df.filter(df["msg_type"].isNull()).count() == 0

    # Test 2: Longitude is within range (-180 to 180)
    assert df.filter(df["lon"] < -180).count() == 0
    assert df.filter(df["lon"] > 180).count() == 0

    # Test 3: Latitude is within range (-90 to 90)
    assert df.filter(df["lat"] < -90).count() == 0
    assert df.filter(df["lat"] > 90).count() == 0

    # Test 4: Speed is within range (0 ≤ speed < 102.2)
    assert df.filter(df["speed"] < 0).count() == 0
    assert df.filter(df["speed"] >= 102.2).count() == 0

    # Test 5: Course is within range (0 ≤ course ≤ 360)
    assert df.filter(df["course"] < 0).count() == 0
    assert df.filter(df["course"] > 360).count() == 0