import json
import os
from datetime import datetime
from pathlib import Path

class ExecutionTracker:
    """
    Allows tracking the execution status of various steps in a data processing pipeline.
    """
    def __init__(self, control_path: str):
        self.control_path = control_path
        os.makedirs(os.path.dirname(self.control_path), exist_ok=True)

    def _get_history(self) -> dict:
        if os.path.exists(self.control_path):
            with open(self.control_path, 'r') as f:
                try:
                    return json.load(f)
                except json.JSONDecodeError:
                    return {}
        return {}
    
    def was_executed(self, step_name: str, reference_date: str) -> bool:
        # Check if a step was executed successfully for a given reference date
        history = self._get_history()
        key = f"{step_name}:{reference_date}"
        return history.get(key) == "SUCCESS"
    
    def register_success(self, step_name: str, reference_date: str):
        # Register a successful execution of a step for a given reference date
        history = self._get_history()
        key = f"{step_name}:{reference_date}"
        history[key] = "SUCCESS"
        history[f"{key}_timestamp"] = datetime.now().isoformat()

        with open(self.control_path, 'w') as f:
            json.dump(history, f, indent=4)