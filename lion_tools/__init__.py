from .src import *
# from .src.DataFrameExtensions import DataFrameExtensions as _DataFrameExtensions

# # Make DataFrameExtensions importable as a submodule
# import sys
# class _ModuleProxy:
#     def __getattr__(self, name):
#         if name == 'DataFrameExtensions':
#             return _DataFrameExtensions
#         raise AttributeError(f"module 'lion_tools.DataFrameExtensions' has no attribute '{name}'")

# sys.modules['lion_tools.DataFrameExtensions'] = _ModuleProxy()

# __all__ = ['DataFrameExtensions', 'start_spark', 'extend_dataframe']
