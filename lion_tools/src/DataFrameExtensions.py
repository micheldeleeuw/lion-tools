import pyspark.sql.functions as F
from pyspark.sql.column import Column
import inspect
import json

class DataFrameExtensions():

    @staticmethod
    def extend_dataframe():

        global DataFrame
        from pyspark.sql import DataFrame

        # Extend DataFrame with new methods
        DataFrame.eCockpit = DataFrameExtensions.cockpit
        DataFrame.eDisplay = DataFrameExtensions.display
        DataFrame.eName = DataFrameExtensions.name
        DataFrame.eSort = DataFrameExtensions.sort
        DataFrame.eSources = DataFrameExtensions.sources

        # Short aliases, pls don't extend these
        DataFrame.eD = DataFrameExtensions.display
        DataFrame.eC = DataFrameExtensions.cockpit

    def __init__(self):
        print('Use extend_dataframe() to extend DataFrame functionality.')
  
    @staticmethod
    def sources(df) -> list:
        """
        Investigate a dataframe and return all tables that source it.
        """

        def _loop_plan(value):
            if isinstance(value, list):
                for list_value in value:
                    _loop_plan(list_value)
            elif isinstance(value, dict):
                if "table" in value.keys() and "database" in value.keys():
                    if "catalog" in value.keys():
                        identified_sources.append(value["catalog"] + "." + value["database"] + "." + value["table"])
                    else:
                        identified_sources.append(value["database"] + "." + value["table"])

                for value_key, value_value in value.items():
                    _loop_plan(value_value)

        identified_sources = []
        _loop_plan(json.loads(df._jdf.queryExecution().analyzed().prettyJson()))

        return identified_sources

    @staticmethod
    def sort_transform_expressions(df, *col_exprs):
        cols = df.columns
        col_exprs = list(col_exprs)

        for i in range(len(col_exprs)):
            col_expr = col_exprs[i]

            if isinstance(col_expr, Column):
                # real column leave it alone, user obviously knows what they are doing
                continue

            if isinstance(col_expr, int) and col_expr < 0:
                col_expr = abs(col_expr)
                descending = True
            elif isinstance(col_expr, str) and col_expr[0] == '-':
                col_expr = col_expr[1:]
                descending = True
            else:
                descending = False

            # recode integer column indices to column names
            if isinstance(col_expr, int):
                col_expr = cols[col_expr - 1]

            # we distinguish real column names versus column expressions
            # to be able to avoid `` around real column names
            if col_expr in cols:
                col_expr = F.col(col_expr)
            else:
                col_expr = F.expr(col_expr)

            if descending:
                col_expr = col_expr.desc()
                
            # put the modified expression back
            col_exprs[i] = col_expr

        return col_exprs

    @staticmethod    
    def sort(df, *col_exprs):
        return df.orderBy(DataFrameExtensions.sort_transform_expressions(df, *col_exprs))

    @staticmethod
    def name(_local_df):
        # we go up max 5 levels to find a variable that holds the dataframe

        for locals in (
            inspect.currentframe().f_back.f_back.f_back.f_back.f_back.f_locals,
            inspect.currentframe().f_back.f_back.f_back.f_back.f_locals,
            inspect.currentframe().f_back.f_back.f_back.f_locals,
            inspect.currentframe().f_back.f_back.f_locals,
            inspect.currentframe().f_back.f_locals,
        ):
            # just return the first name where the value is the same dataframe
            # note that we have use _local_df as the name of the parameter to avoid
            # confusion with the actual dataframe name
            for name, value in locals.items():
                if value is _local_df and name != '_local_df':
                    return name

        return 'unnamed' 

    @staticmethod
    def display(df, *args, **kwargs):
        from .DataFrameDisplay import DataFrameDisplay
        return DataFrameDisplay.display(df, *args, **kwargs)

    @staticmethod
    def cockpit(_local_df, *args, **kwargs):
        from .Cockpit import Cockpit
        return Cockpit.to_cockpit(_local_df, *args, **kwargs)