import pytest
from load_data.data_loader import LoadData
from tests.spark_session_test import spark



def test_spark_session_creation(spark):
    """
    Test that the Spark session is successfully created.

    Verifies:
        - The Spark session fixture returns a non-None object.
        - The Spark session has a valid version attribute set.
    """
    assert spark is not None
    assert spark.version is not None

def test_create_mid(spark):
    """
    This test ensures that the 'mid' column is properly generated from the MMSI column,
    and verifies that the data is correctly loaded from the CSV file.

    Steps:
        1. Load the dataset using the LoadData class.
        2. Check that the 'mid' column exists in the DataFrame.
        3. Ensure that at least some data is loaded (15 records).
        4. Validate that there is one row where the 'mid' value has provided value.

    Args:
        spark (SparkSession): The Spark session used to run the test.

    Asserts:
        - 'mid' column exists in the loaded DataFrame.
        - 15 rows are loaded from the CSV file.
        - one row with 'mid' equal to 232.
    """
    # Assign the class to constant
    loader = LoadData(spark)

    # Create DataFrame
    ais_df = loader.load_datasets("test_file.csv")

    # Tests for 'mid' and row count
    assert "mid" in ais_df.columns  # The 'mid' column should exist
    assert ais_df.count() == 15   # We should have some data loaded

    # Test for the correctness of 'mid' value
    assert ais_df.filter(ais_df["mid"] == 232).count() >= 1 # One or more row with 'mid' equal to 232