from pyspark.sql import SparkSession
import pytest

# Prepare function as fixture
@pytest.fixture(scope="session") # https://docs.pytest.org/en/stable/explanation/fixtures.html
def spark():
    """
    Provides a SparkSession fixture for PySpark tests.

    This fixture initializes a local Spark session once per test session,
    and automatically stops it after all tests are completed. It allows
    for efficient reuse of the same session across multiple test modules.

    Yields:
        SparkSession: A configured SparkSession instance for use in tests.
    """
    # Initialize the session
    spark_session = SparkSession.builder.master("local[*]").appName("test_ais").getOrCreate()

    # The generator ensures that the session is created only once during a test
    yield spark_session

    # and stop the session after test
    spark_session.stop()
