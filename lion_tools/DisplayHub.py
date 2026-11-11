from os import name
import re
import json
import base64
import os
from pathlib import Path
from typing import Any
from .Tools import Tools
import datetime

CONFIG_NAME = "lion_tools_hub.json"
DATA_NAME = "hub_data.js"
MESSAGE_NAME = "message.json"
MESSAGE_DATA_NAME = "message.js"
MAX_TABS = 6
MAX_OUTPUTS = 50

class DisplayHub():

    @staticmethod
    def add_to_display_hub(item: Any, display_hub_path: str, name: str, type: str='html'):
        display_hub = DisplayHub(display_hub_path)
        display_hub.add_output(item, name, type)
        display_hub.update_display_hub()

    @staticmethod
    def set_message(display_hub_path: str, message: str):
        message_data = {
            "message": message,
            "timestamp": datetime.datetime.now().isoformat()
        }
        hub_path = Path(display_hub_path)
        DisplayHub._write_atomic(hub_path / MESSAGE_NAME, json.dumps(message_data))
        # the open index.html polls this script for the status (fetch of json is blocked on file://)
        message_js = f"window.LION_HUB_MESSAGE = {json.dumps(message_data).replace('</', '<\\/')};"
        DisplayHub._write_atomic(hub_path / MESSAGE_DATA_NAME, message_js)

    @staticmethod
    def _write_atomic(path: Path, content: str):
        # write-and-replace so a polling hub page never reads a half-written file
        tmp_path = path.with_name(path.name + ".tmp")
        with open(tmp_path, "w", encoding="utf-8") as f:
            f.write(content)
        os.replace(tmp_path, path)

    @staticmethod
    def _gif_data_uri(name: str) -> str:
        data = (Path(__file__).parent / "templates" / name).read_bytes()
        return "data:image/gif;base64," + base64.b64encode(data).decode("ascii")

    def __init__(self, display_hub_path: str):
        if not Path(display_hub_path).is_dir():
            raise FileNotFoundError(f"Display hub path does not exist or is not a directory: {display_hub_path}")
        
        self.display_hub_path = display_hub_path
        self.config_path = Path(display_hub_path) / CONFIG_NAME
        self.master_html_path = Path(self.display_hub_path) / "index.html"
        self._load_config()

    def _load_config(self):
        if not self.config_path.is_file():
            self._recreate_config()

        with open(self.config_path, "r") as f:
            self.config = json.load(f)

    def _save_config(self):
        with open(self.config_path, "w") as f:
            json.dump(self.config, f)

    def reset(self):
        self._recreate_config()

    def _recreate_config(self):
        with open(self.config_path, "w") as f:
            json.dump({
                "outputs": []
            }, f)
        self._load_config()

    def _delete_unreferenced_outputs(self):
        referenced_files = {o["filename"] for o in self.config["outputs"]}
        for file in Path(self.display_hub_path).glob("*"):
            if (
                file.name not in referenced_files and
                file.name not in (CONFIG_NAME, DATA_NAME, MESSAGE_NAME, MESSAGE_DATA_NAME, "index.html")
            ):
                file.unlink()

    def add_output(self, item: Any, name: str, type: str='html'):
        if type == 'html':
            assert isinstance(item, str), "HTML content must be passed as a string."
        else:
            raise ValueError(f"Unsupported file type for name: {name}")

        # write the output to a file
        now = datetime.datetime.now()
        filename = re.sub(r'[^a-zA-Z0-9_\-\.]', '_', name) + f"_{now:%Y%m%d_%H%M%S%f}.{type}"
        output_path = Path(self.display_hub_path) / filename
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(item)

        # update the config
        self.config["outputs"].insert(0, {
            "name": name,
            "filename": filename,
            "type": type,
            "timestamp": now.isoformat(),
        })
        self._save_config()
        self._limit_outputs()

    def _limit_outputs(self):
        if len(self.config["outputs"]) > MAX_OUTPUTS:
            self.config["outputs"] = self.config["outputs"][:MAX_OUTPUTS]
            self._save_config()

    def update_display_hub(self):
        outputs = [o for o in self.config["outputs"] if o["type"] == "html"]
        # "</" is escaped so the JSON cannot close the surrounding <script> tag
        outputs_json = json.dumps(outputs).replace("</", "<\\/")
        html = (
            Tools.load_file("hub_index_template.html")
            .replace("{max_tabs}", str(MAX_TABS))
            .replace("{outputs_json}", outputs_json)
            .replace("{gif_busy}", DisplayHub._gif_data_uri("hub_busy.gif"))
            .replace("{gif_error}", DisplayHub._gif_data_uri("hub_error.gif"))
            .replace("{gif_done}", DisplayHub._gif_data_uri("hub_done.gif"))
        )
        with open(self.master_html_path, "w", encoding="utf-8") as f:
            f.write(html)

        # the open index.html polls this script for new outputs (fetch of json is blocked on file://)
        with open(Path(self.display_hub_path) / DATA_NAME, "w", encoding="utf-8") as f:
            f.write(f"window.LION_HUB_OUTPUTS = {outputs_json};")

        self._delete_unreferenced_outputs()

        
