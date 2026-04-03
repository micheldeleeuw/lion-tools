from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

class DataFrameSummarize():

    @staticmethod
    def summarize(
            df: DataFrame, 
            *by: str,
            top: int = 5, 
            stats: list[str] = [
                "count_distinct",
                "count_null",  "count_not_null", 
                "min", "max", "avg", "sum",   
            ],
            round_decimals: int = 5
        ) -> DataFrame:

        top = 0 or top
        allowable_stats = ["min", "max", "sum", "avg", "count_null",  "count_not_null",  "count_distinct"]

        assert 0 <= top <= 100, "top must be between 0 and 100"
        assert all(stat in allowable_stats for stat in stats), f"stats must be a list of {allowable_stats}"

        summary = df.eGroup(*by).agg(*stats)
        df_cols = df.columns
        summary_cols = summary.columns

        summary = (
            summary
            .select(
                *by,
                F.explode(
                    F.array(
                        *[
                            F.struct(
                                F.lit(i).alias('column_no'),
                                F.lit(col).alias('column'),
                                *[
                                    F.lit(None).alias(stat)
                                    if f"{stat}_{col}" not in summary_cols
                                    else F.col(f"{stat}_{col}").cast('string').alias(stat)
                                    if stat in ['min', 'max']
                                    else F.round(F.col(f"{stat}_{col}").cast('double'), round_decimals).alias(stat)
                                    if stat in ['avg', 'sum']
                                    else F.col(f"{stat}_{col}").cast('int').alias(stat)
                                    for stat in stats
                                ],
                            )
                            for i, col in enumerate([col for col in df_cols if col not in by])
                        ]
                    )
                ).alias('summary')
            )
            .select(*by, 'summary.*')
        )

        if top > 0:
            top = DataFrameSummarize.top(df, *by, n=top, transpose=True)
            # summary.show()
            # top.show()
            summary = (
                summary
                .join(
                    top,
                    [*[summary[col].eqNullSafe(top[col]) for col in by], summary['column_no'].eqNullSafe(top['column_no'])],
                    'left'
                )
                .select(*[summary[col] for col in summary.columns], *[top[col] for col in top.columns if col.startswith('occurence_')])
            )

        return summary.orderBy(*by, 'column_no')
    

    @staticmethod
    def top(
        df: DataFrame,
        *by: str,
        n: int = 10,
        transpose: bool = False,
    ):
        assert isinstance(n, int), "n must be an integer"
        assert isinstance(transpose, bool), "transpose must be a boolean"
        assert n >=1, "n must be greater than or equal to 1"

        cols = df.columns

        top = (
            df
            .select(
                *by,
                F.explode(
                    F.array(*[
                        F.struct(
                            F.lit(i).alias('column_no'),
                            F.lit(col).alias('column'),
                            F.col(col).cast('string').alias('value')
                        ) for i, col in enumerate([col for col in cols if col not in by])]
                    )
                ).alias('row')
            )
            .select(*by, 'row.*')
            .groupBy(*by, 'column_no', 'column', 'value').agg(F.count('*').alias('count'))
            .withColumn('row_number', F.row_number().over(Window.partitionBy(*by, 'column').orderBy(F.desc('count'))))
            .filter(F.col('row_number') <= n)
        )

        if transpose:
            top = (
                top
                .withColumn('_transpose_id', F.concat(F.lit('occurence_'), F.lpad(F.format_number('row_number', 0), len(str(n)), '0')))
                .withColumn('value', F.concat(F.coalesce(F.col('value'), F.lit('null')), F.lit(' ('), F.format_number('count', 0), F.lit(')')))
                .groupBy(*by, 'column_no', 'column')
                .pivot('_transpose_id')
                .agg(F.first('value'))
            )

        return top.orderBy('column_no', *by)


    

