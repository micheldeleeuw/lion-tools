import pathlib
import tempfile

LION_TOOLS_PATH = pathlib.Path(tempfile.gettempdir(), "lion_tools")
LION_TOOLS_TMP_PATH = LION_TOOLS_PATH.joinpath("tmp")

LION_TOOLS_PATH.mkdir(exist_ok=True)
LION_TOOLS_TMP_PATH.mkdir(exist_ok=True)
