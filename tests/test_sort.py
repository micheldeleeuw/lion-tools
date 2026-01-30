def test_sort():
    from pyspark.sql import SparkSession
    from lion_tools.src.DataFrameExtensions import DataFrameExtensions

    spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
    df = spark.createDataFrame(
        [
            (1, "Alice", 30),
            (2, "Bob", 25),
            (3, "Charlie", 35),
        ],
        ["id", "name", "age"]
    )

    DataFrameExtensions.extend_dataframe()

    # Test sorting by column name
    df_sorted = df.sort("age")
    ages = [row['age'] for row in df_sorted.collect()]
    assert ages == [25, 30, 35], f"Expected [25, 30, 35], got {ages}"

    # Test sorting by column index
    df_sorted = df.sort(3)  # age is the 3rd column
    ages = [row['age'] for row in df_sorted.collect()]
    assert ages == [25, 30, 35], f"Expected [25, 30, 35], got {ages}"

    # Test sorting in descending order
    df_sorted = df.sort(-3)  # age descending
    ages = [row['age'] for row in df_sorted.collect()]
    assert ages == [35, 30, 25], f"Expected [35, 30, 25], got {ages}"

