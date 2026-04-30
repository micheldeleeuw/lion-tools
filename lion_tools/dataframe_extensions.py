from .DataFrameDisplay import DataFrameDisplay as _DataFrameDisplay
from .DataFrameExcel import DataFrameExcel as _DataFrameExcel
from .DataFrameOther import DataFrameOther as _DataFrameOther
from .DataFrameGroup import DataFrameGroup as _DataFrameGroup
from .DataFrameSummary import DataFrameSummary as _DataFrameSummary
from .DataFrameTap import DataFrameTap as _DataFrameTap
from .Cockpit import Cockpit as _Cockpit

# DataFrame extensions
# display
set_display_colors = _DataFrameDisplay.set_display_colors
display = _DataFrameDisplay.display

# regular extensions
examples = _DataFrameOther.examples
name = _DataFrameOther.name
normalize_columns = _DataFrameOther.normalize_columns
remove_empty_columns = _DataFrameOther.remove_empty_columns
round = _DataFrameOther.round
sort = _DataFrameOther.sort
sources = _DataFrameOther.sources
transpose = _DataFrameOther.transpose

# excel
excel = _DataFrameExcel.excel
excel_cockpit = _DataFrameExcel.excel_cockpit

#group
group = _DataFrameGroup.group
sections = _DataFrameGroup.sections

# summary
compare_summaries = _DataFrameSummary.compare_summaries
summary = _DataFrameSummary.summary
top = _DataFrameSummary.top

# tap
tap = _DataFrameTap.tap
tap_end = _DataFrameTap.tap_end

# cockpit
to_cockpit = _Cockpit.to_cockpit

__all__ = [
    'compare_summaries',
    'display',
    'examples',
    'excel',
    'excel_cockpit',
    'group',
    'name',
    'normalize_columns',
    'remove_empty_columns',
    'round',
    'sections',
    'set_display_colors',
    'sort',
    'sources',
    'transpose',
    'summary',
    'tap',
    'tap_end',
    'top',
    'to_cockpit'
]