def start_spark():
    from pyspark.sql import SparkSession
    
    spark = (
        SparkSession.builder
        .master("local[*]")
        .getOrCreate()
    )

    # Suppress verbose Spark logs
    spark.sparkContext.setLogLevel("ERROR")

    return spark
