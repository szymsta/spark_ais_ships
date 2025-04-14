import pytest
from tests.spark_session_test import spark
from tests.test_clean_data.test_data_cleaner import cleaned_data
from tests.test_load_data.test_data_loader import raw_data
from search_data.data_searcher import SearchData


# Test search_ship - Search by MMSI
def test_search_ship_by_mmsi_returns_correct_ship_data(spark, cleaned_data):
    """
    Verifies that the search_ship method returns the correct ship based on MMSI.
    """
    # Create an instance of SearchData
    search_data = SearchData(spark)

    # Define MMSI number
    mmsi_to_search = 289096215

    # Create DF
    search_df = search_data.search_ship(mmsi_to_search, cleaned_data)

    # Test 1: MMSI = 289096215 should be in mmsi_in_search_df
    mmsi_in_search_df = [row["mmsi"] for row in search_df.collect()]
    assert mmsi_to_search in mmsi_in_search_df
