from .DataFrameDisplay import DataFrameDisplay
from .DataFrameExcel import DataFrameExcel
from .DataFrameExtensions import DataFrameExtensions
from .DataFrameGroup import DataFrameGroup
from .DataFrameSummary import DataFrameSummary
from .DataFrameTap import DataFrameTap
from .CockpitLogging import CockpitLogging
from .Tools import Tools
from .Cockpit import Cockpit
from .get_or_create_spark import get_or_create_spark

# extend_dataframe = DataFrameExtensions.extend_dataframe

# display
set_display_defaults = DataFrameDisplay.set_display_defaults
set_display_colors = DataFrameDisplay.set_display_colors
display = DataFrameDisplay.display

# regular extensions
examples = DataFrameExtensions.examples
name = DataFrameExtensions.name
normalize_columns = DataFrameExtensions.normalize_columns
remove_empty_columns = DataFrameExtensions.remove_empty_columns
round = DataFrameExtensions.round
sort = DataFrameExtensions.sort
sources = DataFrameExtensions.sources
transpose = DataFrameExtensions.transpose

# excel
excel = DataFrameExcel.excel
excel_cockpit = DataFrameExcel.excel_cockpit

#group
group = DataFrameGroup.group
sections = DataFrameGroup.sections

# summary
summary = DataFrameSummary.summary
top = DataFrameSummary.top
compare_summaries = DataFrameSummary.compare_summaries

# tap
tap = DataFrameTap.tap
tap_end = DataFrameTap.tap_end


start_cockpit = Cockpit.run