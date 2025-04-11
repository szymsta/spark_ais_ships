import pytest
from pyspark.sql.functions import col
from tests.spark_session_test import spark
from tests.test_clean_data.test_data_cleaner import cleaned_data
from tests.test_load_data.test_data_loader import raw_data
from analyse_data.data_analyzer import AnalyzeData


# Test calculate_country
def test_calculate_country(spark, cleaned_data):
    """
    Test the calculation of the country distribution.

    Verifies:
        - The 'country' and 'count' columns exist in the result.
        - 'Turkey' and 'Norway' are present in the result.
        - 'China' is not present in the result.
    """
    # Create an instance of AnalyzeData
    analyze_data = AnalyzeData(spark)
    
    # Create DF
    country_df = analyze_data.calculate_country(cleaned_data)

    # Test 1: Check if country and count columns exist
    assert "country" in country_df.columns
    assert "count" in country_df.columns

    # Test 2: Ensure 'Turkey' and 'Norway' are in the results and "China" not
    countries_in_result = [row["country"] for row in country_df.collect()]
    assert "Turkey" in countries_in_result
    assert "Norway" in countries_in_result
    assert "China" not in countries_in_result


# Test calculate_dynamic_data
def test_calculate_dynamic_data(spark, cleaned_data):
    """
    Test the calculation of dynamic data by filtering valid message types.

    Verifies:
        - The 'msg_type' column contains only values within the range [1, 2, 3, 18, 19].
    """
    # Define the valid dynamic message types
    valid_msg_types = [1, 2, 3, 18, 19]

    # Create an instance of AnalyzeData
    analyze_data = AnalyzeData(spark)

    # Run the method
    dynamic_df = analyze_data.calculate_dynamic_data(cleaned_data)

    # Test1: Check if all msg_type values in the result are within the valid range
    msg_types_in_result = [row["msg_type"] for row in dynamic_df.collect()]
    assert all(msg_type in valid_msg_types for msg_type in msg_types_in_result)
