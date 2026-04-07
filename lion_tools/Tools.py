import os
import pathlib
from .settings import LION_TOOLS_COCKPIT_PATH
from IPython.display import display as ipython_display


class Tools():

    @staticmethod
    def check_data_type(data_type: str, needed_type: str):
        match needed_type:
            case "num":
                return (
                    data_type in ('int', 'bigint', 'smallint', 'tinyint', 'float', 'double') or
                    data_type.startswith('decimal')
                )
            case "num_int":
                return (
                    data_type in ('int', 'bigint', 'smallint', 'tinyint')
                )
            case "num_non_int":
                return (
                    data_type in ('float', 'double') or
                    data_type.startswith('decimal')
                )
            case _:
                raise ValueError(f"Unsupported needed_type: {needed_type}.")

    @staticmethod
    def load_file(name: str, type='template') -> str:
        if type == 'template':
            path = pathlib.Path(__file__).parent / "templates" / name
        elif type == 'cockpit_json':
            path = LION_TOOLS_COCKPIT_PATH.joinpath(name + ".json")
        elif type == 'cockpit_html':
            path = LION_TOOLS_COCKPIT_PATH.joinpath(name + ".html")
        else:
            path=name

        with open(path, "r", encoding="utf-8") as f:
            return f.read()
        
    @staticmethod
    def on_databricks():
        if os.getenv("DATABRICKS_RUNTIME_VERSION") is not None:
            return True
        return False

    @staticmethod
    def display(content, **kwargs):
        if Tools.on_databricks():
            # use the default display method in Databricks, as it can handle the interactivity
            # and sandboxing better than iframes in that environment
            try:
                _display = eval("display")
            except:
                raise Exception(
                    "Could not find the display function to render the cockpit in Databricks."
                )
            else:
                _display(content)
        else:
            # use the IPython display method in other environments
            ipython_display(content, **kwargs)
