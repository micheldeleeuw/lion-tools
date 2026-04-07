from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

# from lion_tools import DataFrameExtensions
from .DataFrameDisplay import DataFrameDisplay


class DataFrameSummary():

    @staticmethod
    def summary(
            df: DataFrame, 
            *by: str,
            top: int = 5, 
            stats: list[str] = [
                "approx_count_distinct",
                "count_null",  "count_not_null", 
                "min", "max", "avg", "sum",   
            ],
            round_decimals: int = 5,
            include_datatypes: bool = True,
        ) -> DataFrame:

        top = 0 or top
        allowable_stats = [
            "min", "max", "sum", "avg", "avg_null", "count_null",  "count_not_null",
            "count_distinct", "approx_count_distinct"
        ]

        assert 0 <= top <= 100, "top must be between 0 and 100"
        assert all(stat in allowable_stats for stat in stats), f"stats must be a list of {allowable_stats}"

        summary = df.eGroup(*by).agg(*stats)
        df_cols = df.columns
        df_dtypes = dict(df.dtypes)
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
                                F.lit(df_dtypes[col]).alias('datatype'),
                                *[
                                    F.lit(None).alias(stat)
                                    if f"{stat}_{col}" not in summary_cols
                                    else F.col(f"{stat}_{col}").cast('string').alias(stat)
                                    if stat in ['min', 'max']
                                    else F.round(F.col(f"{stat}_{col}").cast('double'), round_decimals).alias(stat)
                                    if stat in ['avg', 'avg_null', 'sum']
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
            top = DataFrameSummary.top(df, *by, n=top, transpose=True)
            summary = (
                summary
                .join(
                    top,
                    [
                        *[summary[col].eqNullSafe(top[col]) for col in by], 
                        summary['column_no'].eqNullSafe(top['column_no']),
                    ],
                    'left'
                )
                .select(
                    *[summary[col] for col in summary.columns], 
                    *[top[col] for col in top.columns if col.startswith('occurence_')]
                )
            )

        if not include_datatypes:
            summary = summary.drop('datatype')

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
    
    @staticmethod
    def compare_summary(
        _df1: DataFrame,
        _df2: DataFrame,
        *by: str,
        stats: list[str] = [
            "approx_count_distinct",
            "count_null",  "count_not_null", 
            "min", "max", "avg", "sum",   
        ],
        color_code_thresholds=[
            ('>= 1.0', 'red'),
            ('>= 0.01', 'yellow'),
        ], 
        round_decimals: int = 5,
        ignore_missing_columns: bool = False,
    ) -> DataFrame:
        
        from .DataFrameExtensions import DataFrameExtensions
        
        by = list(by)
        common_columns = list(set(_df1.columns).intersection(set(_df2.columns)))
        name1 = DataFrameExtensions.name(_df1)
        name2 = DataFrameExtensions.name(_df2)
        name1 = name1 if name1 != 'unnamed' else 'base'
        name2 = name2 if name2 != 'unnamed' else 'compare'
        assert isinstance(color_code_thresholds, list), "color_code_thresholds must be a list in the format [(condition, color_code), ...]"

        if ignore_missing_columns:
            _df1 = _df1.select(*common_columns)
            _df2 = _df2.select(*common_columns)

        missing_columns_1 = list(set(_df2.columns) - set(_df1.columns))
        missing_columns_2 = list(set(_df1.columns) - set(_df2.columns))

        summary1 = DataFrameSummary.summary(_df1, *by, stats=stats, round_decimals=round_decimals, top=0)
        summary2 = DataFrameSummary.summary(_df2, *by, stats=stats, round_decimals=round_decimals, top=0)

        comparison = (
            summary1
            .withColumn('_in_summary1', F.lit(True))
            .alias('summary1')
            .join(
                summary2
                .withColumn('_in_summary2', F.lit(True))
                .alias('summary2'),
                [summary1[col].eqNullSafe(summary2[col]) for col in by + ['column']],
                'full_outer'
            )
            # In code below we calculate the differences between the two summaries, for nummeric values we
            # calculate the relative difference, for string, boolean, date and timestamp values we check
            # if they are the same or not, if not relative difference is set to 100%, if they are the 
            # same it is set to 0%. When values are missing in one of the summaries, we set the difference
            # to 100% to indicate a complete difference.
            .select(
                F.col("summary1.column_no").alias(f'column_no__{name1}'),
                F.col("summary2.column_no").alias(f'column_no__{name2}'),
                *[
                    F.coalesce(F.col(f"summary1.{col}"), F.col(f"summary2.{col}")).alias(col)
                    for col in by + ['column']
                ],
                F.col("summary1.datatype").alias(f'datatype__{name1}'),
                F.col("summary2.datatype").alias(f'datatype__{name2}'),
                F.expr('if(summary1.datatype <=> summary2.datatype, 0.0, 100.0) as datatype__diff_perc'),
                *sum([
                    [
                        F.col(f"summary1.{stat}").alias(f"{stat}__{name1}"),
                        F.col(f"summary2.{stat}").alias(f"{stat}__{name2}"),
                        F.expr(f"""
                            case
                               when ('{stat}' = 'min' or '{stat}' = 'max') and
                                    (summary1.datatype in ('string', 'boolean', 'date', 'timestamp')
                                     or summary2.datatype in ('string', 'boolean', 'date', 'timestamp'))
                               then if(summary1.{stat} <=> summary2.{stat}, 0.0, 100.0)
                               when summary1.{stat} <=> summary2.{stat}
                               then 0.0
                               when summary1.{stat} is null or summary2.{stat} is null
                               then 100.0
                               else
                                    round(
                                        100 *
                                        (
                                            cast(summary1.{stat} as double) - 
                                            cast(summary2.{stat} as double)
                                        ) / 
                                        nullif(cast(summary1.{stat} as double), 0.0),
                                        {round_decimals}
                                    )
                            end as {stat}__diff_perc
                        """)
                    ]
                    for stat in stats
                ], [])
            )
            # single column with the conclusion if statistics are equal
            .transform(
                lambda df: df.withColumn(
                    'result',
                    F.expr(
                        'case when ' + ' and '.join([
                            f"{stat}__diff_perc = 0.0" for stat in stats
                        ] + [f"datatype__diff_perc = 0.0"]) + 
                        ' then "" else "≠" end'
                    )
                )
                .select(
                    *by, F.col(f'column_no__{name1}').alias('column_no'), 'column', 'result', 
                    *[col for col in df.columns if col not in by + ['column', 'result']]
                )
            )
            # set the colors based on the tresholds provided
            .transform(
                DataFrameDisplay.set_colors, 
                *[
                    {
                        'column': "result",
                        'condition': " or ".join([f"{stat}__diff_perc {condition}" for stat in stats]),
                        'color_code': color_code,
                    }
                    for condition, color_code in color_code_thresholds
                ],
                *[
                    {
                        'column': f"{stat}__diff_perc",
                        'condition': f"{stat}__diff_perc {condition}",
                        'color_code': color_code,
                    }
                    for stat in stats
                    for condition, color_code in color_code_thresholds
                ],
            )
            .orderBy(
                *by, 
                F.col(f'column_no__{name1}').asc_nulls_last(),
                F.col(f'column_no__{name2}').asc_nulls_last(),
            )
            .withColumn('_rownum', F.monotonically_increasing_id())
            .drop(f'column_no__{name1}', f'column_no__{name2}')
        )
        
        return comparison


