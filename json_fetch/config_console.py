"""Token"""

import json
import os
from pathlib import Path
from typing import Dict, List
from platformdirs import *
from pprintjson import pprintjson



class Token:
    token_file_name = "token.txt"
    def __init__(self) -> None:
        con_path = ConfigPath()
        con_dir, con_files = con_path.config_dir, con_path.config_files
        token_file = self.find_token_file(con_files)
        self.__token_file = Path(con_dir) / token_file
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

    @staticmethod
    def find_token_file(files):
        """Find token file among config files."""
        found = 0
        out = ""
        target_name = Token.token_file_name
        for f in files:
            # more than 1 found
            if found > 1:
                raise FileExistsError(f"More than one file names {target_name} under config directory. Remove duplicated one.")
            # find token file name in file path
            if target_name in f:
                found += 1
                out = f

        # If fails to find token file
        if not found:
            raise FileNotFoundError("token file doesn't exist under config directory.")

        return out



class ConfigJson:
    """A group of rules of excluded and included."""
    def __init__(self, json_name:str) -> None:
        self.json_name = json_name
        con_path = ConfigPath()
        con_dir, con_files = con_path.config_dir, con_path.config_files
        json_file_path = self.find_json_file(con_files, self.json_name)
        self.json_file = Path(con_dir) / json_file_path
        self.json_default = {"organization": [], "repository": []}

        # if json is empty then write json content to default
        self.set_empty_json_to_default()

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

    def set_empty_json_to_default(self):
        if os.stat(self.json_file).st_size == 0:
            with open(self.json_file, "w+") as f:
                    json.dump(self.json_default, f)

    @staticmethod
    def find_json_file(files, json_name):
        """Find json file among config files."""
        found = 0
        out = ""
        for f in files:
            # more than 1 found
            if found > 1:
                raise FileExistsError(f"More than one file names {json_name} under config directory. Remove duplicated one.")
            # find json file name in file path
            if json_name in f:
                found += 1
                out = f
                break

        # If fails to find token file
        if not found:
            raise FileNotFoundError(f"json file {json_name} doesn't exist under config directory.")

        return out
class ConfigPath:
    def __init__(self) -> None:
        self.app_name = "GatorTracer"
        self.author = "Yanqiao Chen"
        self.config_dir = user_config_dir(self.app_name,self.author) 
        # key -> dirs, value -> list of config files
        # file -> string, folder -> sub_dict
        self.config_files_tree: Dict[List[str]] = {"fetch_scope":["exclude.json", "include.json"],
        "secret":["token.txt"]}

        self.config_files = ConfigPath.parse_path_dict(self.config_files_tree)

    def initialize_config_path(self):

        main_config_path = Path(self.config_dir)

        # make directory of config path
        if not main_config_path.is_dir():
            main_config_path.mkdir()
            print(f"📁 Initializing user config directory {main_config_path}")

        # make sub files in the sub folders from main config path
        for file in self.config_files:
            
            file_path = main_config_path  / file
            
            # make all the parent dirs from the file
            file_path.parents[0].mkdir(parents=True,exist_ok=True)

            # Make the file
            if not file_path.is_file():
                file_path.touch()
                print(f"📁 Initializing user config file {file_path}")
            
    @staticmethod
    def parse_path_dict(path_dict) -> List[str]:
        """Parse path tree into a list of full paths."""
        parsed_paths = []
        for folder in path_dict:
            for content in path_dict[folder]:
                # If content is a folder, it should be a dictionary
                if isinstance(content,dict):
                    # Parse sub-path
                    sub_paths = ConfigPath.parse_path_dict(content) 

                    # Glue the current folder name with the sub-path names
                    sub_files = []
                    for sub_path in sub_paths:
                        full_sub_path = Path(folder) / sub_path
                        sub_files.append(full_sub_path.as_posix())
                    parsed_paths.extend(sub_files)
                # if content is string, it should be a string
                elif isinstance(content, str):
                    content_path = Path(folder) / content
                    parsed_paths.append(content_path.as_posix())
                else:
                    raise TypeError(f"Unexpected type {type(content)} for {content} in the path tree. Should be str for file or dict for dir.")
        return parsed_paths
