from email import message
from pprint import pprint
from .settings import LION_TOOLS_PATH, LION_TOOLS_COCKPIT_PATH
from .settings import cleanup_old_files, cleanup_temp_views
from .DataFrameDisplay import DataFrameDisplay
from .DataFrameExtensions import DataFrameExtensions
from .DataFrameTap import DataFrameTap
from .DataFrameExcel import DataFrameExcel
from datetime import datetime, timedelta
import json
import os
import time
import base64
from IPython.display import display as ipython_display, HTML
import ipywidgets as widgets
from .get_or_create_spark import get_or_create_spark
from collections import deque
from .Tools import Tools

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

        params = json.loads(Tools.load_file(json_file, type='cockpit_json'))

        params["display"] = False
        params["file_path"] = params["html_file"]
        params["allow_additional_parameters"] = True
        pre_timestamp = datetime.now()
        try:
            df = get_or_create_spark().table(
                f"global_temp.{params['temp_view_name']}"
            )
            cls.update_log_panel(f"Loading {params.get('name', 'no name')} ...")
            if "page_length" not in params:
                params["page_length"] = cls.page_length
            DataFrameDisplay.display(df, **params)
        except Exception as e:
            result_html = (
                cls.error_template.replace("{title}", params.get("name", "Error"))
                .replace("{error_type}", type(e).__name__)
                .replace("{error}", str(e))
            )

            with open(params["html_file"], "w", encoding="utf-8") as f:
                f.write(result_html)

            if cls.raise_errors:
                raise
        finally:
            cls.update_log_panel(f" done ({str(timedelta(seconds=round((datetime.now() - pre_timestamp).total_seconds(), 2))).replace('0000', '')})", new_line=False)


    @classmethod
    def initialize(cls):
        # we can do some initialization work here if needed, for now we just clean up old files and temp views
        cleanup_old_files()
        cleanup_temp_views()

        cls.log_template = Tools.load_file("log_template.html", type='template')
        cls.error_template = Tools.load_file("dataframe_error_template.html", type='template')
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
        height = str(int((cls.page_length * 25) + 207)) + 'px'

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

        css_injection = widgets.HTML(value=Tools.load_file("tabs_css_injection.html", type='template'))
        cls.log_panel = widgets.HTML(
            layout=widgets.Layout(
                width="99.9%",
                # height=height,
                # flex=f"0 0 {height}",  # fixed height
                overflow="hidden",
                # margin="2px",
                # padding="0px",
                # border_radius='6px',
            ),
        )
        cls.update_log_panel()

        cls.main_panel = widgets.VBox(
            [css_injection, cls.tabs_panel, cls.log_panel],
            layout=widgets.Layout(
                width="99.9%",
                height="auto",  # Changed to fit total content
                display="flex",
                flex_flow="column",
                background="#f0f0f0",
                margin="0px",
                padding="0px",
            ),
        )

        Tools.display(cls.main_panel, sandbox="allow-scripts allow-same-origin")

    @classmethod
    def clear(cls):
        cleanup_old_files(clean_all=True)
        cleanup_temp_views(clean_all=True)
        
    @classmethod
    def update_tabs_panel(cls):
        # if the initial tab is still there and we have more than 1 tab, remove the initial tab
        if len(cls.tabs) > 1 and cls.init_tab in cls.tabs:
            cls.tabs.remove(cls.init_tab)

        # Keep number of tabs definied by max_tabs by removing duplicates and old tabs if needed
        while len(cls.tabs) > cls.max_tabs:
            names = [tab.get("name") for tab in cls.tabs]
            tab_to_remove = None
            # Search from oldest (end) to newest (start) for a tab whose name appears in a newer tab
            for i in range(len(cls.tabs) - 1, -1, -1):
                if cls.tabs[i].get("name") in names[:i]:
                    tab_to_remove = cls.tabs[i]
                    break
            if tab_to_remove is None:
                tab_to_remove = cls.tabs[-1]  # fall back to the oldest tab
            cls.tabs.remove(tab_to_remove)
            
        # Populate the tabs panel with the content of the tabs and set the titles        
        cls.tabs_panel.children = tuple(tab.get("content") for tab in cls.tabs)
        for i, tab in enumerate(cls.tabs):
            cls.tabs_panel.set_title(i, tab.get("name"))
        
        # move the focus to the newly added tab
        cls.tabs_panel.selected_index = 0
        cls.last_active_time = time.time()

    @classmethod
    def update_log_panel(cls, message: str | list[str] = None, new_line: bool = True):
        message = [message] if isinstance(message, str) else message

        if message:
            cls.append_to_log_lines(message, new_line=new_line)
        
        _log_lines = list(cls.log_lines)
        _log_lines += [""] * (cls.log_length - len(_log_lines) + 1)
        new_log_content = "<br>".join(_log_lines)  # show oldest to newest

        if cls.log_content != new_log_content:
            cls.log_content = new_log_content
            cls.log_panel.value = (
                cls.log_template
                .replace("{log_height}", str(cls.log_length*12*1.2 + 22))  # pixel perfect estimate of log height based on number of lines
                .replace("{log}", cls.log_content)
            )
            cls.last_active_time = time.time()

    @classmethod
    def sync_xlsx_to_tabs(cls):
        overview = cls.get_overview()
        xlsx_files = overview["xlsx"]
        max_xslsx_in_tabs = max([
            '_'.join(tab['id'].split('_')[-3:])
            for tab in cls.tabs if tab.get("type") == "xlsx"
        ] + ['0'])  # in case there are no xlsx tabs yet
        
        new_xlsx_files = [
            xlsx for xlsx in xlsx_files
            if xlsx not in [tab['id'] for tab in cls.tabs]
            and '_'.join(xlsx.split('_')[-3:]) > max_xslsx_in_tabs
        ]

        for xlsx in new_xlsx_files:
            title = '_'.join(xlsx.replace('_lion_tools_tmp_', '').split('_')[:-3]) + '.xlsx'
            html_content = DataFrameExcel.download_button(
                path=str(LION_TOOLS_COCKPIT_PATH.joinpath(xlsx)), 
                display_or_return='return',
                title = title,
            )
            new_tab = {
                "id": xlsx,
                "type": "xlsx",
                "name": title,  # remove prefix and timestamp from name
                "content": widgets.HTML(value=html_content),
            }
            cls.tabs.insert(0, new_tab)  # move newly added tab to the left-most position
            cls.update_tabs_panel()

    @classmethod
    def sync_htmls_to_tabs(cls):
        overview = cls.get_overview()
        htmls = overview["html"]
        lastest_id_in_tabs = max([tab.get("id") for tab in cls.tabs if tab['type'] == 'html'] + ['0'])  # in case there are no html tabs yet
        new_htmls = [html for html in htmls if html > lastest_id_in_tabs][
            -cls.max_tabs :
        ]

        for html in new_htmls:
            # we load the html from file and encode it to add it as hardcoded source to a iframe
            # that is will be the content of the tab
            html_content = Tools.load_file(html, type='cockpit_html')
            params = json.loads(Tools.load_file(html, type='cockpit_json'))
            encoded_html = base64.b64encode(html_content.encode("utf-8")).decode("utf-8")
            page_length = int(params.get("page_length", 15))
            # The vertical scollbar comes from the iframe, we set it to the height of the tab
            max_height = str(int((cls.page_length * 25) + 207 - 10)) + 'px'
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
            cls.update_tabs_panel()

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
        for log in list(cls.monitored_logs.values()):
            update_needed = False

            # Read new lines from the log
            while True:
                line = log['file'].readline()
                if line:
                    update_needed = True
                    cls.append_to_log_lines(line.strip())
                else:
                    break

            # Check if the log has been reset (deleted, truncated or rewritten)
            try:
                if os.path.getsize(log['path']) < log['position']:
                    update_needed = True
                    log['file'].seek(0)
                    log['position'] = log['file'].tell()
                    cls.append_to_log_lines(f'log file {log["log_file"]} was rewritten or truncated.')
            except FileNotFoundError:
                # The log file has been deleted, we stop monitoring it
                log['file'].close()
                cls.monitored_logs.pop(log['log_file'])
                cls.append_to_log_lines(f'log file {log["log_file"]} was deleted.')
                update_needed = True
                continue
            
            # we're done
            log['position'] = log['file'].tell()
            if update_needed:
                cls.update_log_panel()

    @classmethod
    def append_to_log_lines(cls, lines: str | list[str], new_line: bool = True):
        if isinstance(lines, str):
            lines = [lines]

        if new_line:
            # add the current time to each new line
            timestamp = datetime.now().strftime("%H:%M:%S")
            lines = [f"[{timestamp}] - {line}" for line in lines]

        if new_line:
            cls.log_lines.extend(lines)
        else:
            if len(lines) > 1:
                raise ValueError("When new_line is False only a single line is allowed.")
            
            cls.log_lines[-1] += "".join(lines[0])


    @classmethod
    def run(
        cls, 
        timeout: int = 10, 
        tabs: int = 5, 
        clean_start: bool = False, 
        raise_errors: bool = False,
        log_backfill: bool = True,
        size: int = 3,
        page_length: int = None,
        log_length: int = None,
        log_history: int = 200,
        ):
        """
        Cockpit server main loop. This method will be called when the cockpit server is started.
        It will continuously check for new display requests and update the cockpit accordingly.
        """

        page_length = int(size * 3) if page_length is None else page_length
        log_length = int(size * 2.5) if log_length is None else log_length
        
        assert 0.05 <= timeout <= 1440, "timeout must be between 0.05 and 1440 minutes."
        assert 1 <= tabs <= 20, "tabs must be between 1 and 20."
        assert 1 <= size <= 10, "size must be between 1 and 10."
        assert 1 <= page_length <= 100, "page_length must be between 1 and 100."
        assert 1 <= log_length <= 100, "log_length must be between 1 and 100."
        assert 10 <= log_history <= 2000, "log_history must be between 10 and 2000 lines."
        
        if clean_start:
            cls.clear()

        cls.max_tabs = tabs
        cls.raise_errors = raise_errors
        cls.monitored_logs = {}
        cls.log_backfill = log_backfill
        cls.log_lines = deque(maxlen=log_history)
        cls.append_to_log_lines(f"Waiting for logs, timeout set to {timeout} minutes after last activity.")
        cls.log_content = ""
        cls.page_length = page_length
        cls.log_length = log_length
        cls.initialize()

        cls.last_active_time = time.time()
        while True:
            # Reading and processing log files is fast and we want it close to the action
            # in the generating session, so we do it first
            # 1. Find new log files
            cls.find_new_logs()

            # 2. Get new lines from the monitored log files
            cls.process_logs()

            # 3. Add new xlsx files to the tabs
            cls.sync_xlsx_to_tabs()

            # 4. Add new html files to the tabs (both lazy and non-lazy mode)
            cls.sync_htmls_to_tabs()

            # 5. Create html files for new json files (only lazy mode)
            cls.create_html_for_json()

            # 6. House keeping
            if time.time() - cls.last_active_time > timeout * 60:
                cls.append_to_log_lines("Timeout reached, stopping the Cockpit.")
                cls.update_log_panel()
                break

            if time.time() - cls.last_active_time > 0.2:
                time.sleep(0.5)
            elif time.time() - cls.last_active_time > 10:
                time.sleep(2)

    @staticmethod
    def get_overview():
        spark = get_or_create_spark()
        files = os.listdir(LION_TOOLS_COCKPIT_PATH)
        # ensure that displays are processed in the correct order
        files.sort()
        html = [f.replace(".html", "") for f in files if f.endswith(".html")]
        json = [f.replace(".json", "") for f in files if f.endswith(".json")]
        xlsx = [f for f in files if f.endswith(".xlsx")]
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
            "xlsx": xlsx,
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
        if Tools.on_databricks():
            return True
        else:
            return False

    @staticmethod
    def to_cockpit(_df, *args, **kwargs):
        # Dataframe is name __df to be able to find the name of the dataframe
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
        
        # we want the cockpit to determine the page length unless it is explicitly set
        keep_page_length = kwargs.get("p") or kwargs.get("page_length") 

        # as the cockpit will run in a different session, we need to validate and save the parameters
        # for the cockpit to pick up and do the actual display
        params = DataFrameDisplay.display_validate_parameters(
            _df, *args, **kwargs
        )

        if not keep_page_length:
            del params["page_length"]

        if "name" not in params:
            params["name"] = DataFrameExtensions.name(_df)

            if params["name"] == "unnamed":
                sources = DataFrameExtensions.sources(_df)
                if len(sources) == 1:
                    params["name"] = sources[0].split(".")[-1]
                elif len(sources) > 1:
                    params["name"] = sources[0].split(".")[-1] + f" (+{len(sources)-1})"

        if params['add_time_to_name']:
            params["name"] += " - " + datetime.now().strftime("%H:%M:%S")

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
            _df.createOrReplaceGlobalTempView(params["temp_view_name"])
        else:
            kwargs["display"] = False
            kwargs["passthrough"] = False
            kwargs["file_path"] = params["html_file"]
            DataFrameDisplay.display(_df, **kwargs)

        # create the file that informs the Cockpit
        with open(params["json_file"], "w", encoding="utf-8") as f:
            f.write(json.dumps(params))

        if "passthrough" in params and params["passthrough"]:
            return _df
        elif DataFrameTap.tapped and DataFrameTap.tapped['end_on_display']:
            return DataFrameTap.tap_end()

