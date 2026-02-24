from .DataFrameExtensions import DataFrameExtensions


class DataFrameGroup():

    @staticmethod
    def group(df, *by):
        return DataFrameGroup(df, *by)

    def __init__(self, df, *by):
        pass


