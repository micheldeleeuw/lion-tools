from pprint import pprint
from .settings import LION_TOOLS_PATH, LION_TOOLS_COCKPIT_PATH
from .settings import cleanup_old_files, cleanup_temp_views, on_databricks
from .DataFrameDisplay import DataFrameDisplay
from .DataFrameExtensions import DataFrameExtensions
from datetime import datetime
import json
import os
import time
import pathlib
import base64
from pyspark.sql import SparkSession
from IPython.display import display as ipython_display, HTML
import ipywidgets as widgets


class Cockpit:
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

    @classmethod
    def create_html_for_json(cls):

        overview = cls.get_overview()
        if len(overview["new_json"]) == 0:
            return
        else:
            json_file = overview["new_json"][0]

        with open(
            LION_TOOLS_COCKPIT_PATH.joinpath(json_file + ".json"), "r", encoding="utf-8"
        ) as f:
            params = json.load(f)

        params["display"] = False
        params["file_path"] = params["html_file"]
        params["allow_additional_parameters"] = True
        try:
            df = SparkSession.getActiveSession().table(
                f"global_temp.{params['temp_view_name']}"
            )
            cls.update_message_bar(f"Loading {params.get('name', 'no name')}...")
            DataFrameDisplay.display(df, **params)
        except Exception as e:
            with open(
                pathlib.Path(__file__).parent.parent
                / "templates"
                / "dataframe_error_template.html",
                "r",
                encoding="utf-8",
            ) as f:
                template = f.read()

            result_html = (
                template.replace("{title}", params.get("name", "Error"))
                .replace("{error_type}", type(e).__name__)
                .replace("{error}", str(e))
            )

            with open(params["html_file"], "w", encoding="utf-8") as f:
                f.write(result_html)
        finally:
            cls.update_message_bar()

    @classmethod
    def initialize(cls):
        # we can do some initialization work here if needed, for now we just clean up old files and temp views
        cleanup_old_files()
        cleanup_temp_views()

        cls.init_tab = {
            "id": "1999_overview",
            "type": "start",
            "name": "Lion Tools Cockpit",
            "content": widgets.Label(
                "Use the Lion Tools dataframe extension .eC() to show dataframes here."
            ),
            "file": None,
        }

        cls.tabs = [cls.init_tab]

        cls.tabs_panel = widgets.Tab(
            children=[],
            layout=widgets.Layout(
                width="99.9%",
                flex="1 1 auto",
                overflow="auto",
                margin="0px",
                padding="0px",
            ),
        )

        # INJECT CUSTOM CSS TO REMOVE TAB PADDING and set the colors of the tabs
        with open(
            pathlib.Path(__file__).parent.parent
            / "templates"
            / "tabs_css_injection.html",
            "r",
            encoding="utf-8",
        ) as f:
            css_injection = widgets.HTML(value=f.read())

        cls.message_bar = widgets.HTML()
        cls.update_message_bar()

        cls.main_panel = widgets.VBox(
            [css_injection, cls.message_bar, cls.tabs_panel],
            layout=widgets.Layout(
                width="100%",
                height="600px",
                display="flex",
                flex_flow="column",
                background="#f0f0f0",
                margin="0px",
                padding="0px",
            ),
        )
        cls.update_tabs_panel()

        if on_databricks():
            # use the default display method in Databricks, as it can handle the interactivity
            # and sandboxing better than iframes in that environment
            try:
                _display = eval("display")
            except:
                raise Exception(
                    "Could not find the display function to render the cockpit in Databricks."
                )
            else:
                _display(cls.main_panel)
        else:
            # use the IPython display method in other environments
            ipython_display(cls.main_panel, sandbox="allow-scripts allow-same-origin")

    @classmethod
    def clear(cls):
        cleanup_old_files(clean_all=True)
        cleanup_temp_views(clean_all=True)

    @classmethod
    def update_message_bar(cls, message=None):
        message = message or ""
        with open(
            pathlib.Path(__file__).parent.parent
            / "templates"
            / "message_bar_template.html",
            "r",
            encoding="utf-8",
        ) as f:
            cls.message_bar.value = f.read().replace("{message}", message)

    @classmethod
    def update_tabs_panel(cls):
        cls.tabs_panel.children = tuple(tab.get("content") for tab in cls.tabs)
        for i, tab in enumerate(cls.tabs):
            cls.tabs_panel.set_title(i, tab.get("name"))
        # move the focus to the newly added tab
        cls.tabs_panel.selected_index = 0

    @classmethod
    def sync_htmls_to_tabs(cls):
        overview = cls.get_overview()
        htmls = sorted(overview["html"])
        lastest_id_in_tabs = max([tab.get("id") for tab in cls.tabs])
        new_htmls = [html for html in htmls if html > lastest_id_in_tabs][
            -cls.max_tabs :
        ]

        for html in new_htmls:
            html_file = LION_TOOLS_COCKPIT_PATH.joinpath(html + ".html")
            with open(html_file, "r", encoding="utf-8") as f:
                html_content = f.read()
            json_file = LION_TOOLS_COCKPIT_PATH.joinpath(html + ".json")
            with open(json_file, "r", encoding="utf-8") as f:
                params = json.load(f)

            encoded_html = base64.b64encode(html_content.encode("utf-8")).decode(
                "utf-8"
            )

            page_length = int(params.get("page_length", 20))
            max_height = str(int(page_length * 25 + 165)) + "px"
            iframe_html = f"""
                <iframe
                    src="data:text/html;base64,{encoded_html}"
                    width="100%"
                    height="{max_height}"
                    frameborder="0"
                    sandbox='allow-scripts allow-same-origin'
                    style="border: 1px solid #ddd; margin: 0px; padding: 0px;">
                </iframe>
            """
            new_tab = {
                "id": html,
                "type": "html",
                "name": params.get("name", "no name"),
                "content": widgets.HTML(value=iframe_html),
            }
            cls.tabs.insert(
                0, new_tab
            )  # move newly added tab to the left-most position
            # if the initial tab is still there and we have more than 1 tab, remove the initial tab
            if len(cls.tabs) > 1 and cls.init_tab in cls.tabs:
                cls.tabs.remove(cls.init_tab)
            cls.tabs = cls.tabs[: cls.max_tabs]  # keep only the latest max_tabs tabs

            cls.update_tabs_panel()

    @classmethod
    def run(cls, timeout=60, tabs=5, clear=False):
        """
        Cockpit server main loop. This method will be called when the cockpit server is started.
        It will continuously check for new display requests and update the cockpit accordingly.
        """
        if clear:
            cls.clear()

        cls.max_tabs = tabs
        cls.initialize()

        start_time = time.time()
        while True:
            mean_time = time.time()
            cls.sync_htmls_to_tabs()
            cls.create_html_for_json()
            if time.time() - start_time > timeout * 60:
                print("Timeout reached, stopping the Cockpit.")
                break
            if time.time() - mean_time < 0.2:
                time.sleep(0.5)

    @staticmethod
    def get_overview():
        spark = SparkSession.getActiveSession()
        files = os.listdir(LION_TOOLS_COCKPIT_PATH)
        html = [f.replace(".html", "") for f in files if f.endswith(".html")]
        json = [f.replace(".json", "") for f in files if f.endswith(".json")]
        json_with_html = [f for f in json if f in html]
        new_json = [f for f in json if f not in json_with_html]
        if spark is not None:
            temp_views = [
                view.name.replace("_view", "")
                for view in spark.catalog.listTables("global_temp")
                if view.name.startswith("_lion_tools_tmp_")
            ]
        else:
            temp_views = []
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
        pprint(Cockpit.get_overview())

    @staticmethod
    def is_lazy_supported():
        """
        Currently only Databricks is supported for lazy mode.
        """
        if on_databricks():
            return True
        else:
            return False

    @staticmethod
    def to_cockpit(_local_df, *args, **kwargs):
        # Dataframe is name _local_df to be able to find the name of the dataframe
        # by going up the stack and looking for a variable with the same value.

        # clean old stuff up before doing anything, to avoid filling up
        # the temp directory and global temp view namespace
        cleanup_old_files()
        cleanup_temp_views()

        # validate parameters and set defaults
        if "file_path" in kwargs:
            raise Exception(
                "file_path parameter is not supported in display_in_cockpit, use display instead."
            )

        # as the cockpit will run in a different session, we need to validate and save the parameters
        # for the cockpit to pick up and do the actual display
        params = DataFrameDisplay.display_validate_parameters(
            _local_df, *args, **kwargs
        )

        if "name" not in params:
            params["name"] = DataFrameExtensions.name(_local_df)

            if params["name"] == "unnamed":
                sources = DataFrameExtensions.sources(_local_df)
                if len(sources) == 1:
                    params["name"] = sources[0].split(".")[-1]
                elif len(sources) > 1:
                    params["name"] = sources[0].split(".")[-1] + f" (+{len(sources)-1})"

        params["id"] = "_lion_tools_tmp_" + datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        params["temp_view_name"] = params["id"] + "_view"
        params["html_file"] = str(
            LION_TOOLS_COCKPIT_PATH.joinpath(params["id"] + ".html")
        )
        params["json_file"] = str(
            LION_TOOLS_COCKPIT_PATH.joinpath(params["id"] + ".json")
        )

        # if not running in lazy mode, we create the html in the session and send it to the cockpit,
        # otherwise we just send the parameters and let the cockpit do the html creation work
        if ("lazy" in kwargs and kwargs["lazy"]) or (
            "lazy" not in kwargs and Cockpit.is_lazy_supported()
        ):
            # create a global temp view for the dataframe, so the cockpit can access the data
            _local_df.createOrReplaceGlobalTempView(params["temp_view_name"])
        else:
            kwargs["display"] = False
            kwargs["passthrough"] = False
            kwargs["file_path"] = params["html_file"]
            DataFrameDisplay.display(_local_df, **kwargs)

        # create the file that informs the Cockpit
        with open(params["json_file"], "w", encoding="utf-8") as f:
            f.write(json.dumps(params))

        if "passthrough" in params and params["passthrough"]:
            return _local_df
