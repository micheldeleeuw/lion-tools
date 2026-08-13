import re
from .DataFrameOther import DataFrameOther
from pyspark.sql import DataFrame
from pyspark.sql import Column
import pyspark.sql.functions as F
import pyspark.sql.window as W
from typing import Self
from functools import reduce
from .Tools import Tools


class DataFrameGroup:
    pivot_separator = "___pivot___"
    pivot_column = "___pivot_column___"
    pivot_value_separator = "__"
    pivot_total_left_column = "!!!!___pivot_total_left___"
    pivot_total_right_column = "~~~~___pivot_total_right___"

    @staticmethod
    def group(df: DataFrame, *by: str, **kwargs) -> "DataFrameGroup":
        return DataFrameGroup(df, *by, **kwargs)
    
    @staticmethod
    def sections(
        df: DataFrame, 
        *section_columns: str,
        sort_by: list[str] = [],
    ) -> DataFrame:
        from .DataFrameGroup import DataFrameGroup

        return (
            DataFrameGroup(df, '*', sort_by=sort_by)
            .totals(*section_columns, sections=True)
            .agg()
        )

    def __init__(
        self,
        df: DataFrame,
        *by: str,
        add_rownum: bool = False,
        round: int | bool = 8,
        sort_by: list[str] = [],
    ):
        sort_by = [sort_by] if isinstance(sort_by, str) else sort_by
        self.df = df
        self.columns = df.columns

        if by == ("*",):
            self.by = ["*"]
            self.by_strings = ["*"]
            self.sort_by = sort_by
        else:
            self.by = DataFrameOther.transform_column_expressions(
                df, *by, include_sort=False
            )
            self.by_strings = [Tools.col_name(col) for col in self.by]
            self.sort_by = (
                [
                    (i + 1)
                    * (-1 if isinstance(col, str) and col.startswith("-") else 1)
                    for i, col in enumerate(by)
                ]
                if sort_by == []
                else sort_by
            )

        self.columns_aggregable = [
            col for col in self.columns if col not in self.by_strings
        ]
        self.columns_nummeric = [
            dtype[0] for dtype in df.dtypes if Tools.check_data_type(dtype[1], "num")
        ]
        self.add_rownum = add_rownum
        self.round = round
        self._pivot = False
        self.pivot_columns = []
        self.pivot_totals_by = []
        self.pivot_type = None
        self.totals_by = []
        self.sections = False
        self.sub_totals = False
        self.grand_total = False

    @staticmethod
    def _normalize_name(name: str) -> str:
        # Replace the specific characters with an underscore
        name = re.sub(r'[ !"#$%&\'()*+,\-./`]', "_", name.lower())
        name = re.sub(r"_+", "_", name)
        return name.strip("_")

    @staticmethod
    def single_aggregation_functions() -> list[str]:
        return [
            "min",
            "max",
            "sum",
            "avg",
            "avg_null",
            "count",
            "count_distinct",
            "approx_count_distinct",
            "count_null",
            "count_not_null",
            "first",
            "last",
            "collect_set",
            "collect_list",
        ]

    def prepare_pivot_column(self) -> None:
        # transform the pivot columns to strings and allow null values and make life easier
        self.df = (
            self.df
            .withColumns({
                column: F.ifnull(F.col(column).cast("string"), F.lit("null")) for column in self.pivot_columns
            })
        )

        # implement pivot totals by exploding the data over the desired totals
        if self.pivot_totals_by != []:
            # first, determine the levels
            pivot_totals_replace_to_totals = [
                [col for col in self.pivot_columns if col not in by]
                for by in self.pivot_totals_by
            ]
            
            # add the original columns as last level of totals
            pivot_totals_replace_to_totals.append([])
            
            # apply the explode
            self.df = (
                self.df
                .withColumn(
                    '___pivot_totals_explode___', 
                    F.array(*[F.struct(*[
                        F.col(column).alias(column) 
                        if column not in replace_to_total
                        else F.lit(self.pivot_total_right_column if self.pivot_totals_position == "right" else self.pivot_total_left_column).alias(column)
                        for column in self.pivot_columns
                    ]) for replace_to_total in pivot_totals_replace_to_totals])
                )
                .drop(*self.pivot_columns)
                .withColumn('___pivot_totals_explode___', F.explode('___pivot_totals_explode___'))
                .selectExpr('*', '___pivot_totals_explode___.*')
                .drop('___pivot_totals_explode___')
            )

        # Concatenate one or multiple pivot columns into a single column
        self.df = (
            self.df.withColumn(
                self.pivot_column,
                F.concat_ws(
                    self.pivot_value_separator, 
                    *[F.col(column) for column in self.pivot_columns]
                ),
            )
        )

        if self.pivot_type != "default":
            # Add the pivot separator at the end of the pivot column to be able to identify
            # pivotted columns in the result and to be able to rename and sort them properly.
            self.df = (
                self.df.withColumn(
                    self.pivot_column,
                    F.concat(self.pivot_column, F.lit(self.pivot_separator)),
                )
            )

    def pivot(self, *columns: str, pivot_type: str = "column_grouped") -> Self:
        assert self.by_strings != ["*"], "Pivoting is not supported when grouping by all columns."
        assert len(columns) > 0, "At least one pivot column must be provided."
        assert pivot_type in ["column_grouped", "pivot_grouped", "default"]
        assert all(
            column in self.columns for column in columns
        ), f"Pivot columns must be one of the columns in the DataFrame. Available columns: {self.columns}."
        assert all(
            column not in self.by_strings for column in columns
        ), "Pivot columns cannot be one of the grouping columns."

        self._pivot = True
        self.pivot_columns = columns
        self.pivot_type = pivot_type

        return self

    def pivot_totals(
        self,
        *by: str,
        sub_totals: bool | None = None,
        grand_total: bool | None = None,
        header: str = 'total',
        position: str = 'right',
    ) -> "DataFrameGroup":
        
        by: list[str] = [
            Tools.col_name(col)
            for col in DataFrameOther.transform_column_expressions(self.df, *by, include_sort=False)
        ]

        assert not self.pivot_columns == [], "Apply pivot before setting pivot totals."
        assert all(
            column in self.pivot_columns for column in by
        ), f"All by variables must be part of the pivot columns. Available pivot columns: {self.pivot_columns}."
        assert not all(
            column in by for column in self.pivot_columns
        ), "By variables cannot be all pivot columns."
        assert not (by != [] and sub_totals), "Sub totals cannot be applied when by variables are provided."
        assert position in ("left", "right"), "Position must be either 'left' or 'right'."

        # Totals are applied :
        # - When by variables are provided, apply totals only on that level, grand total is
        #   optional, but not standard
        # - When nothing is provided, apply totals at every level incl. grand total
        # - When only grand total is provided, apply only grand total
        # - When only sub totals is provided, apply only sub totals
        
        self.pivot_totals_by = by
        self.pivot_totals_header = header
        self.pivot_totals_position = position

        if by != []: # case 1
            sub_totals = False
            grand_total = grand_total or False
        elif sub_totals is None and grand_total is None: # case 2
            sub_totals = True
            grand_total = True
        elif grand_total and sub_totals is None: # case 3
            sub_totals = False
        elif sub_totals and grand_total is None: # case 4
            grand_total = False
        else:
            sub_totals = True if sub_totals is None else sub_totals
            grand_total = True if grand_total is None else grand_total

        if by != []:
            self.pivot_totals_by = [by]
        elif sub_totals:
            self.pivot_totals_by = [list(self.pivot_columns[0:i+1]) for i in range(len(self.pivot_columns)-1)]
        else:
            self.pivot_totals_by = []

        if grand_total:
            self.pivot_totals_by.append([])

        return self

    def agg(
        self,
        *aggs: str,
        alias: bool = False,
        normalize_column_names: bool = False,
        round: int | None = None,
    ) -> DataFrame:

        self.alias = alias
        self.normalize_column_names = normalize_column_names
        self.round = round if round is not None else self.round

        assert not (
            self.by_strings == ["*"]
            and not (self.sections or self.sub_totals or self.grand_total)
            and len(aggs) > 0
        ), "When grouping by all columns, no aggregation functions can be provided unless totals are requested."

        self.aggs = (
            [F.count("*").alias("count")]
            if len(aggs) == 0
            and (len(self.by) > 0 or self.sub_totals or self.grand_total)
            else aggs
        )

        # Force alias when more than one single aggregation function is requested
        if (
            len([
                value
                for value in aggs
                if isinstance(value, str)
                and value in self.single_aggregation_functions()
            ]) > 1
        ):
            self.alias = True

        self._rebuild_aggregates()
        self._get_aggregates()
        self._rename_pivot_columns()
        self._sort_result()

        return self.result

    def rename_pivot_totals_column(self, col: str) -> str:            
        for pivot_total_column in [self.pivot_total_left_column, self.pivot_total_right_column]:
            # first replace repeating pivot total columns with a single one as that looks nicer
            double_pattern = pivot_total_column + self.pivot_value_separator + pivot_total_column
            # new_pattern = pivot_total_column + self.pivot_value_separator + ' '
            new_pattern = ' ' + self.pivot_value_separator + pivot_total_column
            while double_pattern in col:
                # replace right to left
                # col = new_pattern.join(col.rsplit(double_pattern, 1))
                
                # replace left to right
                col = col.replace(double_pattern, new_pattern)
            col = col.replace(pivot_total_column, self.pivot_totals_header)

        return col

    def _rename_pivot_columns(self) -> None:
        result_columns = self.result.columns
        pivot_values = []
        column_names = []
        result_pivot_columns = [col for col in result_columns if col.find(self.pivot_separator) >= 0]
        result_non_pivot_columns = [col for col in result_columns if col.find(self.pivot_separator) < 0]

        for col in result_pivot_columns:
            splitted = col.split(
                self.pivot_separator + "_"
            )  # additional _ comes from the regular pivot
            if len(splitted) > 1:
                column_name, pivot_value = splitted[0], splitted[1]
            else:
                column_name, pivot_value = splitted[0], 'thiswillnotbeused'
            if pivot_value not in pivot_values:
                pivot_values.append(pivot_value)
            if column_name not in column_names:
                column_names.append(column_name)

        if self.pivot_column is False or self.pivot_type == "default":
            # we're good
            pass
        elif len([col for col in result_columns if col.endswith(self.pivot_separator)]) > 0:
            # only one column got pivotted, so we can just rename it to the pivot column value
            self.result = self.result.withColumnsRenamed({
                col: col.replace(f"{self.pivot_separator}", "")
                for col in result_columns
                if col.endswith(self.pivot_separator)
            })
        elif self.pivot_type == "pivot_grouped":
            # multiple columns got pivotted, so rename and rearrange the column names to have the pivot
            # value at the end of the column name
            self.result = self.result.selectExpr(
                *[f"`{col}`" for col in result_non_pivot_columns],
                *[
                    f"`{col}{self.pivot_separator}_{pivot_value}` as `{col}__{pivot_value}`"
                    for col in column_names
                    for pivot_value in pivot_values
                ],
            )
        elif self.pivot_type == "column_grouped":
            # multiple columns got pivotted, so rename and rearrange the column names to have the pivot
            # value at the beginning of the column name
            self.result = self.result.selectExpr(
                *[f"`{col}`" for col in result_non_pivot_columns],
                *[
                    f"`{col}{self.pivot_separator}_{pivot_value}` as `{pivot_value}__{col}`"
                    for pivot_value in pivot_values
                    for col in column_names
                ],
            )

        # rename pivot totals columns if needed
        if self.pivot_totals_by != []:
            self.result = self.result.withColumnsRenamed({
                col: self.rename_pivot_totals_column(col)
                for col in self.result.columns
                if col.find(self.pivot_total_left_column) >=0 or col.find(self.pivot_total_right_column) >= 0
            })

    def _sort_result(self) -> None:
        if self.add_rownum or self.sections or self.sub_totals or self.grand_total:
            sort_by = DataFrameOther.transform_column_expressions(
                self.result, *self.sort_by
            )
            self.result = (
                self.result
                # to be confirmed in test that new code works
                # .withColumn("_rownum", F.row_number().over(W.Window.orderBy(
                # F.expr('if(_totals_type < 9, 0, 1)'), *self.totals_by, '_totals_type', *sort_by)
                # ))
                .orderBy(
                    F.expr("if(_totals_type < 9, 0, 1)"),
                    *self.totals_by,
                    "_totals_type",
                    *sort_by,
                ).withColumn("_rownum", F.monotonically_increasing_id())
            )

            self.result = (
                self.result.transform(
                    lambda df: (
                        df.withColumns(
                            {
                                Tools.col_name(col): F.expr(
                                    f"if(_totals_type not in (3), `{Tools.col_name(col)}`, null)"
                                )
                                for col in self.totals_by
                            }
                        )
                        if len(self.totals_by) > 0
                        else df
                    )
                )
                .withColumns(
                    {
                        col: F.expr(f"if(_totals_type not in (4, 5), `{col}`, null)")
                        for col in self.result.columns
                        if col not in ("_rownum", "_totals_type")
                    }
                )
                .withColumn("_rownum", F.expr("_rownum + (_totals_type / 10)"))
                .withColumn(
                    "",
                    F.expr("case when _totals_type in (3, 9) then '+' else null end"),
                )
                .orderBy("_rownum")
            )
        else:
            self.result = DataFrameOther.sort(self.result, *self.sort_by)

        if not (self.sections or self.sub_totals or self.grand_total):
            self.result = self.result.drop("_totals_type")

    def _get_aggregates(self) -> None:
        def apply_grouping(by, aggs, totals_type) -> DataFrame:
            if by is None:
                # group by all columns is what we defined as no grouping at all
                agg = self.df
            elif aggs is None:
                agg = self.df.select(*by).distinct()
            elif self._pivot:
                agg = self.df.groupBy(*by).pivot(self.pivot_column).agg(*aggs)
            else:
                agg = self.df.groupBy(*by).agg(*aggs)

            return agg.withColumn("_totals_type", F.expr(totals_type))

        # prepare pivot column
        if self._pivot:
            self.prepare_pivot_column()

        # the result is build up by unioning the different levels of aggregation.
        result = []

        # regular aggregation
        if self.by_strings == ["*"]:
            result.append(apply_grouping(None, None, "1"))
        else:
            result.append(apply_grouping(self.by, self.aggs, "2"))

        # sub totals, sections and grand total
        if self.sub_totals:
            result.append(
                apply_grouping(self.totals_by, self.aggs, "explode(array(3, 4))")
            )

        if self.sections:
            result.append(apply_grouping(self.totals_by, None, "5"))

        if self.grand_total:
            result.append(apply_grouping([], self.aggs, "9"))

        # union all result together
        result_df = reduce(
            lambda x, y: x.unionByName(y, allowMissingColumns=True), result
        )

        if not self.round is False:
            result_df = DataFrameOther.round(result_df, self.round)

        self.result = result_df

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
                        result_expression = f"avg(coalesce(`{col}`, 0.0))"
                    elif agg == "count_distinct":
                        result_expression = f"count(distinct(`{col}`))"
                    elif agg == "count_null":
                        result_expression = (
                            f"sum(case when `{col}` is null then 1 else 0 end)"
                        )
                    elif agg == "count_not_null":
                        result_expression = (
                            f"sum(case when `{col}` is not null then 1 else 0 end)"
                        )
                    elif col in self.columns_nummeric or agg in (
                        "max",
                        "min",
                        "count",
                        "first",
                        "collect_set",
                        "collect_list",
                        "approx_count_distinct",
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
                agg = (
                    "sorted_columns() as columns" if agg == "sorted_columns()" else agg
                )
                agg = agg.replace(
                    "sorted_columns()", f'"{", ".join(sorted(self.columns))}"'
                )
                agg = "columns() as columns" if agg == "columns()" else agg
                agg = agg.replace("columns()", f'"{", ".join(self.columns)}"')
                agg = (
                    f"{agg} as `{self._normalize_name(agg)}`"
                    if agg.find(" as ") == -1
                    else agg
                )
                aggs.append([1, i, 0, agg])

            # 3. We assume it is a column expression
            else:
                aggs.append([1, i, 0, agg])

        aggs = sorted(aggs, key=lambda x: x[0] * 100000 + x[2] * 1000 + x[1])
        aggs = [agg[3] for agg in aggs]
        aggs = [
            agg if str(type(agg)).find(".Column") > 0 else F.expr(agg) for agg in aggs
        ]
        self.aggs = aggs

    def totals(
        self,
        *by: str,
        sections: bool | None = None,
        sub_totals: bool | None = None,
        grand_total: bool | None = None,
    ) -> "DataFrameGroup":

        by: list[str] = DataFrameOther.transform_column_expressions(
            self.df, *by, include_sort=False
        )

        assert not (
            sections and sub_totals
        ), "Sections and sub_totals cannot be used together."
        assert not (
            by == [] and (sections or sub_totals)
        ), "Sections and sub_totals are not supported without by variables."
        assert not (
            by == [] and grand_total is False
        ), "Grand total is only option when no by variables are provided."
        assert self.by_strings == ["*"] or all(
            Tools.col_name(col) in self.by_strings for col in by
        ), f"All by variables must be part of the grouping variables. Available grouping variables: {self.by_strings}."

        if by == []:
            grand_total = True
        else:
            sub_totals = True if not (sections or sub_totals) else sub_totals
            grand_total = True if sub_totals and grand_total is None else grand_total

        self.totals_by = by
        self.sections = sections or False
        self.sub_totals = sub_totals or False
        self.grand_total = grand_total or False

        return self

    def count(self, **kwargs) -> DataFrame:
        return self.agg("count(*) as count", **kwargs)

    def __getattr__(self, name):
        if name in self.single_aggregation_functions():

            def wrapper(*args, **kwargs):
                if len(args) > 0:
                    assert all(
                        isinstance(arg, str) for arg in args
                    ), "All arguments must be strings representing column names."
                    assert all(arg in self.columns_aggregable for arg in args), (
                        "All arguments must be column names"
                        f"that can be aggregated. Available columns: {self.columns_aggregable}"
                    )
                    self.columns_aggregable = args
                return self.agg(name, **kwargs)

            return wrapper
        else:
            raise AttributeError(f"'DataFrameGroup' object has no attribute '{name}'")
