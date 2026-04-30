from .DataFrameDisplay import DataFrameDisplay
from .DataFrameExcel import DataFrameExcel
from .DataFrameOther import DataFrameOther
from .DataFrameGroup import DataFrameGroup
from .DataFrameSummary import DataFrameSummary
from .DataFrameTap import DataFrameTap
from .Cockpit import Cockpit

# DataFrame extensions
# display
set_display_colors = DataFrameDisplay.set_display_colors
display = DataFrameDisplay.display

# regular extensions
examples = DataFrameOther.examples
name = DataFrameOther.name
normalize_columns = DataFrameOther.normalize_columns
remove_empty_columns = DataFrameOther.remove_empty_columns
round = DataFrameOther.round
sort = DataFrameOther.sort
sources = DataFrameOther.sources
transpose = DataFrameOther.transpose

# excel
excel = DataFrameExcel.excel
excel_cockpit = DataFrameExcel.excel_cockpit

#group
group = DataFrameGroup.group
sections = DataFrameGroup.sections

# summary
compare_summaries = DataFrameSummary.compare_summaries
summary = DataFrameSummary.summary
top = DataFrameSummary.top

# tap
tap = DataFrameTap.tap
tap_end = DataFrameTap.tap_end

# cockpit
to_cockpit = Cockpit.to_cockpit

__all__ = [
    'set_display_colors',
    'display',
    'examples',
    'name',
    'normalize_columns',
    'remove_empty_columns',
    'round',
    'sort',
    'sources',
    'transpose',
    'excel',
    'excel_cockpit',
    'group',
    'sections',
    'compare_summaries',
    'summary',
    'top',
    'tap',
    'tap_end',
    'to_cockpit'
]