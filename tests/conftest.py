import os
import sys
import pytest
import logging
from pyspark.sql import SparkSession

# MUST be set before importing pyspark
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["SPARK_LOG_LEVEL"] = "ERROR"

# Optional but helps with JVM noise
os.environ["JAVA_TOOL_OPTIONS"] = "-Dlog4j2.disable.jmx=true"

from lion_tools import extend_dataframe
extend_dataframe()

@pytest.fixture(scope="session")
def spark() -> SparkSession:
    # Silence Python-side Spark logging
    logging.getLogger("py4j").setLevel(logging.ERROR)
    logging.getLogger("pyspark").setLevel(logging.ERROR)

    spark = (
        SparkSession.builder
        .master("local[*]")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.extraJavaOptions", "-Dlog4j.rootCategory=ERROR,console")
        .config("spark.executor.extraJavaOptions", "-Dlog4j.rootCategory=ERROR,console")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")
    # we add a separator to the spark logs to make it easier to spot where the real logs start
    print("\n---------------------------------------------------------------------------------------------------------\n")
    return spark
