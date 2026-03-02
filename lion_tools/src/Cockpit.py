from pprint import pprint
from .settings import LION_TOOLS_PATH, LION_TOOLS_COCKPIT_PATH
from .settings import cleanup_old_files, cleanup_temp_views, on_databricks
from .DataFrameDisplay import DataFrameDisplay
from .DataFrameExtensions import DataFrameExtensions
from .DataFrameTap import DataFrameTap
from datetime import datetime
import json
import os
import time
import pathlib
import base64
from IPython.display import display as ipython_display, HTML
import ipywidgets as widgets
from .get_or_create_spark import get_or_create_spark

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
            df = get_or_create_spark().table(
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

            if cls.raise_errors:
                raise
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
            "content": widgets.HTML(
                "<div style='padding: 10px;'>Use the Lion Tools dataframe extension .eC() to show dataframes here.</div>"
            ),
            "file": None,
        }

        cls.tabs = [cls.init_tab]

        height = str(int((cls.page_length * 25) + 197)) + 'px'

        cls.tabs_panel = widgets.Tab(
            children=[],
            layout=widgets.Layout(
                width="99.9%",
                height=height,
                flex=f"0 0 {height}",  # fixed height
                overflow="hidden", # handle scroll inside iframes
                margin="2px",
                padding="0px",
                border_radius='6px',
            ),
        )
        cls.update_tabs_panel()

        # INJECT CUSTOM CSS TO REMOVE TAB PADDING and set the colors of the tabs
        with open(
            pathlib.Path(__file__).parent.parent
            / "templates"
            / "tabs_css_injection.html",
            "r",
            encoding="utf-8",
        ) as f:
            css_injection = widgets.HTML(value=f.read())

        # cls.message_bar = widgets.HTML()
        # cls.update_message_bar()

        cls.log_panel = widgets.HTML(
            # value='',
            # layout=widgets.Layout(
            #         width="99.9%",
            #         height=str(cls.log_height) + "px",
            #         flex=f'0 0 auto',
            #         border='1px solid #d3dae4',
            #         border_radius='6px',
            #         padding='10px',
            #         background='#f7f9fc',
            #         overflow_x='auto',  # horizontal scrollbar when needed
            #         overflow_y='auto'   # (optional) vertical scrollbar
            #     )
        )

        cls.update_log_panel()

        cls.main_panel = widgets.VBox(
            [css_injection, cls.tabs_panel, cls.log_panel],
            # [css_injection, cls.message_bar, cls.tabs_panel, cls.log_panel],
            layout=widgets.Layout(
                width="100%",
                height="auto",  # Changed to fit total content
                display="flex",
                flex_flow="column",
                background="#f0f0f0",
                margin="0px",
                padding="0px",
            ),
        )

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
    def update_message_bar(cls, message="&nbsp;"):
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
    def update_log_panel(cls):
        new_log_content = "<br>".join(cls.log_lines[-100:])  # show oldest to newest
        if cls.log_content == new_log_content:
            return
        
        cls.log_content = new_log_content
        cls.log_lines = cls.log_lines[-300:]  # keep only the latest 300 lines in memory to avoid memory issues
        with open(
            pathlib.Path(__file__).parent.parent
            / "templates"
            / "log_template.html",
            "r",
            encoding="utf-8",
        ) as f:
            template = f.read()
        cls.log_panel.value = (
            template
            .replace("{log_height}", str(cls.log_height-30))
            .replace("{log}", cls.log_content)
        )

    @classmethod
    def sync_htmls_to_tabs(cls):
        overview = cls.get_overview()
        htmls = overview["html"]
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
            # print(cls.tabs)

    @classmethod
    def find_new_logs(cls):
        log_files = cls.get_overview()["log"]
        for log_file in log_files:
            if log_file not in cls.monitored_logs:
                f = open(LION_TOOLS_COCKPIT_PATH.joinpath(log_file), "r", encoding="utf-8")
                if not cls.log_backfill:
                    # move the pointer to the end of the file, so we only read new lines
                    f.seek(0, os.SEEK_END)
                
                cls.monitored_logs[log_file] = {
                    "log_file": log_file,
                    "path": LION_TOOLS_COCKPIT_PATH.joinpath(log_file),
                    "file": f,
                    "position": f.tell()
                }

    @classmethod
    def process_logs(cls):
        for log in cls.monitored_logs.values():
            update_needed = False

            # Read new lines from the log
            while True:
                line = log['file'].readline()
                if line:
                    update_needed = True
                    cls.log_lines.append(line.strip())
                else:
                    break

            # Check if the log has been reset (truncated or rewritten)
            if os.path.getsize(log['path']) < log['position']:
                update_needed = True
                log['file'].seek(0)
                log['position'] = log['file'].tell()
                cls.log_lines.append(f'log file {log["log_file"]} was rewritten or truncated.')
            
            # we're done
            log['position'] = log['file'].tell()
            if update_needed:
                cls.update_log_panel()

    @classmethod
    def run(
        cls, 
        timeout: int = 60, 
        tabs: int = 5, 
        clean_start: bool = False, 
        raise_errors: bool = False, 
        log_backfill: bool = False, 
        page_length: int = 15,
        log_height: int = 200
        ):
        """
        Cockpit server main loop. This method will be called when the cockpit server is started.
        It will continuously check for new display requests and update the cockpit accordingly.
        """
        if clean_start:
            cls.clear()

        cls.max_tabs = tabs
        cls.raise_errors = raise_errors
        cls.monitored_logs = {}
        cls.log_backfill = log_backfill
        cls.log_lines = ["waiting for logs..."]
        cls.log_content = ""
        cls.page_length = page_length
        cls.log_height = log_height
        cls.initialize()

        start_time = time.time()
        while True:
            mean_time = time.time()
            # Reading and processing log files is fast and we want it close to the action
            # in the generating session, so we do it first
            # 1. Find new log files
            cls.find_new_logs()

            # 2. Get new lines from the monitored log files
            cls.process_logs()

            # 3. Add new html files to the tabs (both lazy and non-lazy mode)
            cls.sync_htmls_to_tabs()

            # 4. Create html files for new json files (only lazy mode)
            cls.create_html_for_json()
            if time.time() - start_time > timeout * 60:
                print("Timeout reached, stopping the Cockpit.")
                break
            if time.time() - mean_time < 0.2:
                time.sleep(0.5)

    @staticmethod
    def get_overview():
        spark = get_or_create_spark()
        files = os.listdir(LION_TOOLS_COCKPIT_PATH)
        # ensure that displays are processed in the correct order
        files.sort()
        html = [f.replace(".html", "") for f in files if f.endswith(".html")]
        json = [f.replace(".json", "") for f in files if f.endswith(".json")]
        log = [f for f in files if f.endswith(".log")]
        json_with_html = [f for f in json if f in html]
        new_json = [f for f in json if f not in json_with_html]
        temp_views = [
            view.name.replace("_view", "")
            for view in spark.catalog.listTables("global_temp")
            if view.name.startswith("_lion_tools_tmp_")
        ]
        orphan_temp_views = [view for view in temp_views if view not in json]
        return {
            "files": files,
            "temp_views": temp_views,
            "json_with_html": json_with_html,
            "new_json": new_json,
            "html": html,
            "log": log,
            "orphan_temp_views": orphan_temp_views,
            "cockpit_path": str(LION_TOOLS_COCKPIT_PATH),
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
        elif DataFrameTap.tapped and DataFrameTap.tapped['end_on_display']:
            return DataFrameTap.tap_end()
