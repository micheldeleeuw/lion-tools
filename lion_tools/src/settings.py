import pathlib
import tempfile
import time

from .start_spark import start_spark

LION_TOOLS_PATH = pathlib.Path(tempfile.gettempdir(), "lion_tools")
LION_TOOLS_COCKPIT_PATH = LION_TOOLS_PATH.joinpath("cockpit")
# LION_TOOLS_COCKPIT_DISPLAYS_PATH = LION_TOOLS_COCKPIT_PATH.joinpath("dataframe_displays.log")
LION_TOOLS_PATH_CLEANUP_AGE_SECONDS = 60 * 60 * 24  # 24 hours

LION_TOOLS_PATH.mkdir(exist_ok=True)
LION_TOOLS_COCKPIT_PATH.mkdir(exist_ok=True)

def cleanup_old_files(clean_all=False):
    cutoff = time.time() - LION_TOOLS_PATH_CLEANUP_AGE_SECONDS

    for file in LION_TOOLS_PATH.rglob('*'):
        # print("Checking file:", file)
        if file.is_file() and (clean_all or file.stat().st_mtime < cutoff):
            file.unlink()


def cleanup_temp_views(keep_last=10, clean_all=False):
    """
    Delete global temp views starting with _lion_tools_tmp_ except for the last N created.
    """
    # Get all global temp views
    spark = start_spark()
    all_views = spark.catalog.listTables("global_temp")
    
    # Filter for lion_tools temp views and sort by name (which includes timestamp)
    lion_tools_views = [
        view.name for view in all_views 
        if view.name.startswith("_lion_tools_tmp_")
    ]
    lion_tools_views.sort()
    
    # Delete all except the last N
    views_to_delete = lion_tools_views[:-keep_last] if len(lion_tools_views) > keep_last else []
    if clean_all:
        views_to_delete = lion_tools_views
    
    for view_name in views_to_delete:
        try:
            spark.catalog.dropGlobalTempView(view_name)
        except Exception:
            pass  # Silently ignore errors
    