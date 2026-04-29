def get_or_create_spark():
    """Return an existing Spark session if available, else create a local one.

    Resolution order:
      1. A `spark` variable in the caller's global namespace (and up the stack).
         This catches notebook-injected sessions (Databricks, Jupyter) and
         Spark Connect / Databricks Connect sessions created elsewhere.
      2. The active classic PySpark session via `SparkSession.getActiveSession()`.
      3. The active Spark Connect session, if `pyspark.sql.connect` is importable.
      4. A new local Spark session as a last resort.

    Works transparently with both classic Spark and Spark Connect.
    """
    import inspect

    # 1. Walk up the call stack looking for a `spark` global.
    for frame_info in inspect.stack():
        candidate = frame_info.frame.f_globals.get("spark")
        if _looks_like_spark_session(candidate):
            return candidate

    # 2. Classic PySpark active session.
    try:
        from pyspark.sql import SparkSession
        active = SparkSession.getActiveSession()
        if active is not None:
            return active
    except Exception:
        SparkSession = None  # type: ignore[assignment]

    # 3. Spark Connect active session.
    try:
        from pyspark.sql.connect.session import SparkSession as ConnectSession
        active_connect = ConnectSession.getActiveSession()  # type: ignore[attr-defined]
        if active_connect is not None:
            return active_connect
    except Exception:
        pass

    # 4. Fall back to creating a local classic session.
    if SparkSession is None:
        from pyspark.sql import SparkSession  # re-import for clarity
    spark = (
        SparkSession.builder
        .master("local[*]")
        .getOrCreate()
    )

    # Suppress verbose Spark logs (only available on classic sessions).
    try:
        spark.sparkContext.setLogLevel("ERROR")
    except Exception:
        pass

    return spark


def _looks_like_spark_session(obj) -> bool:
    """Duck-typed check for both classic and Spark Connect SparkSession."""
    if obj is None:
        return False
    cls_name = type(obj).__name__
    module = type(obj).__module__ or ""
    return cls_name == "SparkSession" and module.startswith("pyspark.sql")
