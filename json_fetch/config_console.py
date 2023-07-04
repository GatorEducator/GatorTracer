"""Token"""

import os
from pathlib import Path
import json
from typing import Dict
from pprintjson import pprintjson


class Token:
    def __init__(self) -> None:
        self.__token_file = (
            Path(__file__).parent.resolve() / "config" / "token.txt"
        )  # TODO: file path should change as program running as pypi project
        self.__token = self.__token_file.read_text()

    def token_exists(self):
        if_token_exists = self.__token != ""
        return if_token_exists

    def set_token(self, token):
        with open(self.__token_file, "w") as f:
            f.write(token)
        self.__token = token
        return True

    def remove_token(self):
        with open(self.__token_file, "r") as f:
            if not f.read():
                return False

        with open(self.__token_file, "w") as f:
            f.write("")
        self.__token = ""
        return True

    # Probably shouldn't allow users to call this function??
    def get_token(self):
        return self.__token


class ConfigJson:
    """A group of rules of excluded and included."""

    def __init__(self, json_file) -> None:
        self.json_file = json_file
        self.json_default = {"organization": [], "repository": []}

    def write_json(self, content):
        with open(self.json_file, "w") as f:
            json.dump(content, f)

    def default_json(self):
        with open(self.json_file, "w") as f:
            json.dump(self.json_default, f)     

    def parse_json(self) -> Dict:
        with open(self.json_file, "r") as f:
            js_dict = json.load(f)
        return js_dict
