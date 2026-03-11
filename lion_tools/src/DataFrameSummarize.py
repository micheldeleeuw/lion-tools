from pyspark.sql import DataFrame

class DataFrameSummarize():

    @staticmethod
    def summarize(
            df: DataFrame, 
            top: int = 5, 
            stats: list[str] = ["min", "max", "sum", "avg", "count_null",  "count_not_null"]
        ) -> DataFrame:

        top = 0 or top
        allowable_stats = ["min", "max", "sum", "avg", "count_null",  "count_not_null",  "count_distinct"]

        assert 0 <= top <= 1000, "top must be between 0 and 1000"
        assert all(stat in allowable_stats for stat in stats), f"stats must be a list of {allowable_stats}"

        summary = df.eGroup().agg(*stats)

        return summary

