import pytest
from pyspark.sql import SparkSession

from main import get_product_category_pairs


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder.appName("TestProductCategory")
        .master("local[*]")
        .getOrCreate()
    )


def test_product_category_join(spark):
    products_data = [(1, "Bread"), (2, "Milk"), (3, "Butter"), (4, "Cheese")]

    categories_data = [(10, "Dairy Products"), (20, "Bakery")]

    product_category_data = [(1, 20), (2, 10), (3, 10)]

    products_df = spark.createDataFrame(products_data, ["product_id", "product_name"])
    categories_df = spark.createDataFrame(
        categories_data, ["category_id", "category_name"]
    )
    product_category_df = spark.createDataFrame(
        product_category_data, ["product_id", "category_id"]
    )

    result_df = get_product_category_pairs(
        products_df, categories_df, product_category_df
    )

    expected_data = [
        ("Bread", "Bakery"),
        ("Milk", "Dairy Products"),
        ("Butter", "Dairy Products"),
        ("Cheese", None),
    ]
    expected_df = spark.createDataFrame(
        expected_data, ["product_name", "category_name"]
    )

    assert result_df.collect() == expected_df.collect()
