from pprint import pprint
from .settings import LION_TOOLS_PATH, LION_TOOLS_COCKPIT_PATH
from .settings import cleanup_old_files
from .settings import cleanup_temp_views
from .DataFrameDisplay import DataFrameDisplay
from datetime import datetime
import json
import os
import time
import pathlib
from pyspark.sql import SparkSession
from IPython.display import HTML as display_HTML
from IPython.display import display
import ipywidgets as widgets


class Cockpit():
    """
    The Lion Tools cockpit is a simple web application that displays the results 
    of Python/PySpark sessions running on the same machine, making the code you 
    run in those sessions more interactive and easier to work with. The cockpit is 
    designed to be simple and lightweight, and to work with minimal configuration. 

    Steps to use the cockpit:
    1. Start the cockpit by running `lion_tools cockpit` in your terminal. This
       will start the cockpit server and open the cockpit in that specific Python
       session, preferably in a notebook environment.
    2. In your Python/PySpark session, use the `display_in_cockpit` method to display 
       dataframes in the cockpit. For example:
            from lion_tools import DataFrameExtensions
            DataFrameExtensions.display_in_cockpit(my_dataframe)
    3. The cockpit will automatically pick up the display request and show the dataframe.
       You can interact with the dataframe, view its schema, and see the code that generated it.
    """


    @staticmethod
    def create_html_for_json(json_file):
        from .DataFrameDisplay import DataFrameDisplay

        with open(LION_TOOLS_COCKPIT_PATH.joinpath(json_file + '.json'), 'r', encoding='utf-8') as f:
            params = json.load(f)

        params['display'] = False 
        params['file_path'] = params['html_file']
        params['allow_additional_parameters'] = True
        try:
            df = SparkSession.getActiveSession().table(f"global_temp.{params['temp_view_name']}")
            DataFrameDisplay.display(df, **params)
        except Exception as e:
            with open(pathlib.Path(__file__).parent.parent / "templates" / "dataframe_error_template.html", 'r', encoding='utf-8') as f:
                template = f.read()
            
            result_html = (
                template
                .replace("{title}", params.get('name', 'Error'))
                .replace("{error_type}", type(e).__name__)
                .replace("{error}", str(e))
            )

            with open(params['html_file'], 'w', encoding='utf-8') as f:
                f.write(result_html)


    @classmethod
    def initialize(cls):
        # we can do some initialization work here if needed, for now we just clean up old files and temp views
        cleanup_old_files()
        cleanup_temp_views()

        cls.tabs = [
            {
                "name": "Lion Tools Cockpit",
                "content": widgets.Label("Use the Lion Tools method .eC() to show dataframes here."),
                "file": None,
            },
        ]

        cls.tabs_panel = widgets.Tab(
            children=[tab.get('content') for tab in cls.tabs],
            layout=widgets.Layout(width='99,9%', flex='1 1 auto', overflow='auto', margin="10px")
        )
        for i, tab in enumerate(cls.tabs):
            cls.tabs_panel.set_title(i, tab.get('name'))

        cls.main_panel = widgets.VBox(
            [cls.tabs_panel],
            layout=widgets.Layout(
                width='100%',
                height='600px',
                display='flex',
                flex_flow='column',
                background='#f0f0f0',
                margin='10px;'
            )
        )

        display(cls.main_panel)


    @classmethod
    def sync_htmls_to_tabs(cls):
        htmls = cls.get_overview()['html']



    @classmethod
    def run(cls, timeout=60):
        """
        Cockpit server main loop. This method will be called when the cockpit server is started.
        It will continuously check for new display requests and update the cockpit accordingly.
        """

        cls.initialize()

        start_time = time.time()
        while True:
            

            # update tabs with new html files
            cls.sync_htmls_to_tabs()


            # create html for new_json files
            overview = cls.get_overview()
            if len(overview['new_json']) > 0:
                json_file = overview['new_json'][0]
                cls.create_html_for_json(json_file)
            
            if time.time() - start_time > timeout * 60:
                break

            time.sleep(0.5)


    @staticmethod
    def get_overview():
        spark = SparkSession.getActiveSession()
        files = os.listdir(LION_TOOLS_COCKPIT_PATH)
        html = [f.replace('.html', '') for f in files if f.endswith('.html')]
        json = [f.replace('.json', '') for f in files if f.endswith('.json')]
        json_with_html = [f for f in json if f in html]
        new_json = [f for f in json if f not in json_with_html]
        temp_views = [
            view.name.replace('_view', '') for view in spark.catalog.listTables("global_temp")
            if view.name.startswith("_lion_tools_tmp_")
        ]
        orphan_temp_views = [view for view in temp_views if view not in json]
        return {
            "files": files,
            "temp_views": temp_views,
            "json_with_html": json_with_html,
            "new_json": new_json,
            "html": html,
            "orphan_temp_views": orphan_temp_views,
        }

    @staticmethod
    def print_status():
        status = Cockpit.get_status()
        print("Cockpit status:")

        print(f"Json with HTML file: {len(status['json_with_html'])}")
        for f in status['json_with_html']:
            print(f"  - {f}")

        print(f"\nJSON with temp views: {len(status['json_with_temp_view'])}")
        for f in status['json_with_temp_view']:
            print(f"  - {f}")

        print(f"\nJSON with no HTML or temp view: {len(status['new_json_files'])}")
        for f in status['new_json_files']:
            print(f"  - {f}")

        print(f"\nOrphan temp views: {len(status['orphan_temp_views'])}")
        for view in status['orphan_temp_views']:
            print(f"  - {view.name}")

    @staticmethod
    def is_lazy_supported():
        """
        Currently only Databricks is supported for lazy mode.
        """
        # Check Databricks-specific environment variable
        if "DATABRICKS_RUNTIME_VERSION" in os.environ:
            return True
        else:
            return False

    @staticmethod
    def to_cockpit(_local_df, *args, **kwargs):
        # Dataframe is name _local_df to be able to find the name of the dataframe
        # by going up the stack and looking for a variable with the same value.
        from .DataFrameExtensions import DataFrameExtensions

        # clean old stuff up before doing anything, to avoid filling up
        # the temp directory and global temp view namespace
        cleanup_old_files()
        cleanup_temp_views()

        # validate parameters and set defaults
        if 'file_path' in kwargs:
            raise Exception("file_path parameter is not supported in display_in_cockpit, use display instead.")

        # as the cockpit will run in a different session, we need to validate and save the parameters
        # for the cockpit to pick up and do the actual display
        params = DataFrameDisplay.display_validate_parameters(_local_df, *args, **kwargs)

        if 'name' not in params:
            params['name'] = DataFrameExtensions.dataframe_name(_local_df)
        params['id'] = "_lion_tools_tmp_" + datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        params['temp_view_name'] = params['id'] + '_view'
        params['html_file'] = str(LION_TOOLS_COCKPIT_PATH.joinpath(params['id'] + '.html'))
        params['json_file'] = str(LION_TOOLS_COCKPIT_PATH.joinpath(params['id'] + '.json'))

        # if not running in lazy mode, we create the html in the session and send it to the cockpit, 
        # otherwise we just send the parameters and let the cockpit do the html creation work
        if ('lazy' in kwargs and kwargs['lazy']) or ('lazy' not in kwargs and Cockpit.is_lazy_supported()):
            # create a global temp view for the dataframe, so the cockpit can access the data
            _local_df.createOrReplaceGlobalTempView(params['temp_view_name'])
        else:
            kwargs['display'] = False
            kwargs['passthrough'] = False
            kwargs['file_path'] = params['html_file']
            DataFrameDisplay.display(_local_df, **kwargs)

        # create the file that informs the Cockpit
        with open(params['json_file'], 'a', encoding='utf-8') as f:
            f.write(json.dumps(params))
        
        if 'passthrough' in params and params['passthrough']:
            return _local_df




    

