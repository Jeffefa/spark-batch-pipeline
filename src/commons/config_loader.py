import json
import os
from pathlib import Path

class ConfigLoader:
    def __init__(self, config_path: str = "pipeline_config.json"):
        # get file pipeline_config.json absolute path
        self.base_path = Path(os.path.abspath(config_path)).parent
        self.config_data = self._load_file(config_path)
        self.folders = self.config_data.get("FOLDERS", {})
        self.settings = self.config_data.get("PIPELINE_SETTINGS", {})

    def _load_file(self, file_path: str) -> dict:
        # Read/Load the file if exists
        try:
            with open(file_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            raise Exception(f"Critical Error: Config file not found at {file_path}")
        
    def get_path(self, key: str) -> str:
        # Return the full path for the given key
        relative_path = self.folders.get(key)
        if not relative_path:
            raise KeyError(f"Path key '{key}' not found in FOLDERS configuration.")
        return str((self.base_path / relative_path.replace("../", "")).resolve())
    
    def get_setting(self, key: str):
        # Return the setting value for the given key
        return self.settings.get(key)