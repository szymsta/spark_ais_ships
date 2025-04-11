import pytest
from pyspark.sql.functions import col
from tests.spark_session_test import spark
from tests.test_clean_data.test_data_cleaner import cleaned_data
from tests.test_load_data.test_data_loader import raw_data
from search_data.data_searcher import SearchData