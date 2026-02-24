def get_or_create_spark():
    from pyspark.sql import SparkSession
    
    spark = SparkSession.getActiveSession()

    if spark is None:
        spark = (
            SparkSession.builder
            .master("local[*]")
            .getOrCreate()
        )

    # Suppress verbose Spark logs
    spark.sparkContext.setLogLevel("ERROR")

    return spark
