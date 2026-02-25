import re
from .DataFrameExtensions import DataFrameExtensions
from pyspark.sql import DataFrame
from pyspark.sql import Column
import pyspark.sql.functions as F
import pyspark.sql.window as W

class DataFrameGroup():

    def __init__(self, df: DataFrame, *by: list[str], **kwargs):
        self.df = df
        self.sort_by = DataFrameExtensions.transform_column_expressions(df, *by)
        self.by = DataFrameExtensions.transform_column_expressions(df, *by, include_sort=False)
        self.by_strings = [col._jc.toString() for col in self.by]
        self.columns = df.columns
        self.columns_aggregable = [col for col in self.columns if col not in self.by_strings]
        self.columns_nummeric = [
            dtype[0] for dtype in df.dtypes
            if dtype[1] in ("double", "integer", "int", "short", "long", "float", "bigint")
            or dtype[1].find("decimal") > -1
        ]
        self.totals_by = []
        self.totals_grand_total = False
        self.totals_keep_rownum = False
        self.add_rownum = kwargs.get("add_rownum", False)


    # def _normalize_aggregate_column_names(self):
    #     self.result = self.result.eNormalizeColumnNames(
    #         columns=[col for col in self.result.columns if col not in self.by]
    #     )

    @staticmethod
    def _normalize_name(name: str) -> str:
        # Replace the specific characters with an underscore
        name = re.sub(r'[ !"#$%&\'()*+,\-./`]', '_', name.lower())
        name = re.sub(r'_+', '_', name)
        return name.strip('_')
    
    @staticmethod
    def single_aggregation_functions() -> list[str]:
        return (
            "min", "max", "sum", "avg", "avg_null", "count", "count_distinct",
            "count_null", "count_not_null", "first", "last", "collect_set", "collect_list",
        )

    def agg(self, *aggs: str, **kwargs) -> DataFrame:
        self.alias = kwargs.get("alias", False)
        self.normalize_column_names = kwargs.get("normalize_column_names", False)
        self.aggs = [F.count("*").alias("count")] if len(aggs) == 0 else aggs

        # Force alias when more than one single aggregation function is requested
        if len([
            value for value in aggs
            if isinstance(value, str) and value in self.single_aggregation_functions()
        ]) > 1:
            self.alias = True

        self._rebuild_aggregates()

        result = (
            self.df
            .groupBy(*self.by)
            .agg(*self.aggs)
            .withColumn("_totals_type", F.lit(1))
        )

        if self.totals_by != [] or self.totals_grand_total:
            result = self.add_totals(result)

        if self.add_rownum or self.totals_by != [] or self.totals_grand_total:
            result = (
                result
                .withColumn("_rownum", F.row_number().over(W.Window.orderBy(
                    F.expr('if(_totals_type in (1, 2), 0, 1)'), *self.totals_by, '_totals_type', *self.sort_by)
                ))
                .withColumn("_rownum", F.expr("_rownum + (_totals_type / 10)"))
                .orderBy('_rownum')
            )
        else:
            result = result.orderBy(*self.sort_by)

        return result.drop("_totals_type", "_rownum" if not self.add_rownum else '_none')

    def add_totals(self, result: DataFrame) -> DataFrame:
        if self.totals_by != []:
            result = (
                result
                .unionByName(
                    self.df
                    .groupBy(*self.totals_by)
                    .agg(*self.aggs)
                    .withColumn("_totals_type", F.lit(2)),
                    allowMissingColumns=True,
                )
            )

        if self.totals_grand_total:
            result = (
                result
                .unionByName(
                    self.df
                    .groupBy()
                    .agg(*self.aggs)
                    .withColumn("_totals_type", F.lit(3)),
                    allowMissingColumns=True,
                )
            )

        return result
        
    def _rebuild_aggregates(self) -> None:
        """
        We (re)build the aggregation expressions into expressions that Spark understands.
        1. If it is string with a single aggregation function without parameters
           then that function is applied to all columns for which that function
           makes sense.
        2. Otherwise, if it is a string, we assume is in SQL expression that can
           be applied directly. The only thing we do is that we implement sorted_columns()
           and columns() functionality.
        3. Otherwise, we assume the expression is already a column expression that can
           be applied directly

        Note that we implement each aggregate result columns as an array as we want
        to sort the columns after their determination.
        """

        aggs = []
        for i, agg in enumerate(self.aggs):
            # 1. single aggregation function
            if isinstance(agg, str) and agg in self.single_aggregation_functions():
                for j, col in enumerate(self.columns_aggregable):
                    result_expression = None

                    if col in self.columns_nummeric and agg == "avg_null":
                        result_expression = f"avg(coalesce(`{col}`, 0))"
                    elif agg == "count_distinct":
                        result_expression = f"count(distinct(`{col}`))"
                    elif agg == "count_null":
                        result_expression = f"sum(case when `{col}` is null then 1 else 0 end)"
                    elif agg == "count_not_null":
                        result_expression = f"sum(case when `{col}` is not null then 1 else 0 end)"
                    elif col in self.columns_nummeric or agg in (
                        "max", "min", "count", "first", "collect_set", "collect_list",
                    ):
                        result_expression = f"{agg}(`{col}`)"
                    else:
                        # nummeric aggregation on non nummeric columns are not included
                        pass

                    if result_expression:
                        result_column_name = f"{agg}_{col}" if self.alias else col
                        aggs.append(
                            [2, i, j, f"{result_expression} as `{result_column_name}`"]
                        )

            # 2. SQL expression (string), add sorted_columns() and columns() functionality.
            elif isinstance(agg, str):
                agg = 'sorted_columns() as columns' if agg == "sorted_columns()" else agg
                agg = agg.replace("sorted_columns()", f'"{", ".join(sorted(self.columns))}"')
                agg = 'columns() as columns' if agg == "columns()" else agg
                agg = agg.replace("columns()", f'"{", ".join(self.columns)}"')
                agg = f"{agg} as `{self._normalize_name(agg)}`" if agg.find(" as ") == -1 else agg
                aggs.append([1, i, 0, agg])

            # 3. We assume it is a column expression
            else:
                aggs.append([1, i, 0, agg])

        aggs = sorted(aggs, key=lambda x: x[0] * 100000 + x[2] * 1000 + x[1])
        aggs = [agg[3] for agg in aggs]

        aggs = [agg if isinstance(agg, Column) else F.expr(agg) for agg in aggs]
        self.aggs = aggs

    def totals(self, by: str | list[str] = None, grand_total: bool = None) -> 'DataFrameGroup':
        by = by or []
        by = [by] if isinstance(by, str) else by
        by = DataFrameExtensions.transform_column_expressions(self.df, *by, include_sort=False)
        assert all(col._jc.toString() in self.by_strings for col in by), ("All by variables must be part of the grouping variables."
            f" Available grouping variables: {self.by_strings}. ")

        grand_total = True if not by else grand_total
        grand_total = grand_total or False

        self.totals_by = by
        self.totals_grand_total = grand_total

        return self

    def __getattr__(self, name):
        if name in self.single_aggregation_functions():
            def wrapper(*args, **kwargs):
                if len(args) > 0:
                    assert all(isinstance(arg, str) for arg in args), "All arguments must be strings representing column names."
                    assert all(arg in self.columns_aggregable for arg in args), ("All arguments must be column names"
                        f"that can be aggregated. Available columns: {self.columns_aggregable}")
                    self.columns_aggregable = args
                return self.agg(name, **kwargs)
            return wrapper
        else:
            raise AttributeError(f"'DataFrameGroup' object has no attribute '{name}'")
