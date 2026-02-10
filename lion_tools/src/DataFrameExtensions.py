import pyspark.sql.functions as F
from pyspark.sql.column import Column
import inspect
from .DataFrameDisplay import DataFrameDisplay
from .Cockpit import Cockpit

class DataFrameExtensions():

    @staticmethod
    def extend_dataframe():

        global DataFrame
        from pyspark.sql import DataFrame

        # Extend DataFrame with new methods
        DataFrame.eDisplay = DataFrameExtensions.display
        DataFrame.eSort = DataFrameExtensions.sort
        DataFrame.eDisplayCockpit = DataFrameExtensions.display_cockpit

        # Short aliases
        DataFrame.eD = DataFrameExtensions.display
        DataFrame.eDc = DataFrameExtensions.display_cockpit

    def __init__(self):
        print('Use extend_dataframe() to extend DataFrame functionality.')
  
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
    def dataframe_name(_local_df):
        # we go up max 5 levels to find a dataframe name

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
        DataFrameDisplay.display(df, *args, **kwargs)

    @staticmethod
    def display_cockpit(_local_df, *args, **kwargs):
        Cockpit.display_cockpit(_local_df, *args, **kwargs)