from pyspark.sql import SparkSession


def get_product_category_pairs(products_df, categories_df, product_category_df):
    """
    Returns a DataFrame with "Product Name - Category Name" pairs and products without categories.

    :param products_df: DataFrame with columns [product_id, product_name]
    :param categories_df: DataFrame with columns [category_id, category_name]
    :param product_category_df: DataFrame with columns [product_id, category_id]
    :return: DataFrame with columns [product_name, category_name] (including products without categories)
    """

    # Join product_category to products
    product_with_categories = products_df.join(
        product_category_df, on="product_id", how="left"
    )

    # Join categories
    full_info = product_with_categories.join(
        categories_df, on="category_id", how="left"
    ).select("product_name", "category_name")

    return full_info


def main():
    spark = (
        SparkSession.builder.appName("ProductCategoryJoin")
        .master("local[*]")
        .getOrCreate()
    )

    # Sample data
    products_data = [(1, "Bread"), (2, "Milk"), (3, "Butter"), (4, "Cheese")]

    categories_data = [(10, "Dairy Products"), (20, "Bakery")]

    product_category_data = [
        (1, 20),  # Bread — Bakery
        (2, 10),  # Milk — Dairy
        (3, 10),  # Butter — Dairy
        # Cheese without category
    ]

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

    result_df.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
