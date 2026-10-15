from os import name
import re
import json
from pathlib import Path
from typing import Any
from .Tools import Tools
import datetime

CONFIG_NAME = "lion_tools_hub.json"
MAX_TABS = 6
MAX_OUTPUTS = 50

class DisplayHub():

    @staticmethod
    def add_to_display_hub(item: Any, display_hub_path: str, name: str, type: str='html'):
        display_hub = DisplayHub(display_hub_path)
        display_hub.add_output(item, name, type)
        display_hub.update_display_hub()

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
            if file.name not in referenced_files and file.name != CONFIG_NAME and file.name != "index.html":
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
        )
        with open(self.master_html_path, "w", encoding="utf-8") as f:
            f.write(html)

        self._delete_unreferenced_outputs()

