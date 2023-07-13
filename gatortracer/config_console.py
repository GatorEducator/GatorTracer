"""Configuration related."""
# pylint:disable = invalid-name
import json
import os
from pathlib import Path
from typing import Dict, List

from platformdirs import user_config_dir

ENCODING = "utf-8"


class Token:
    """Saved token."""

    # pylint: disable = line-too-long
    token_file_name = "token.txt"

    def __init__(self) -> None:
        """Initialize Token instance."""
        con_path = ConfigPath()
        con_dir, con_files = con_path.config_dir, con_path.config_files
        token_file = self.find_token_file(con_files)
        self.__token_file = Path(con_dir) / token_file
        self.__token = self.__token_file.read_text()

    def token_exists(self):
        """Check the existence of token."""
        if_token_exists = self.__token != ""
        return if_token_exists

    def set_token(self, token):
        """Set a new saving token."""
        with open(self.__token_file, "w", encoding=ENCODING) as f:
            f.write(token)
        self.__token = token
        return True

    def remove_token(self):
        """Remove saved token."""
        with open(self.__token_file, "r", encoding=ENCODING) as f:
            if not f.read():
                return False

        with open(self.__token_file, "w", encoding=ENCODING) as f:
            f.write("")
        self.__token = ""
        return True

    def get_token(self):
        """Get the saved token."""
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
                raise FileExistsError(
                    f"More than one file names {target_name} under config directory. Remove duplicated one."
                )
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

    def __init__(self, json_name: str) -> None:
        """Initialize ConfigJson instance.

        Args:
            json_name: The name of configuration json
        """
        self.json_name = json_name
        con_path = ConfigPath()
        con_dir, con_files = con_path.config_dir, con_path.config_files
        json_file_path = self.find_json_file(con_files, self.json_name)
        self.json_file = Path(con_dir) / json_file_path
        self.json_default = {"organization": [], "repository": []}

        # if json is empty then write json content to default
        self.set_empty_json_to_default()

    def write_json(self, content):
        """Write Json."""
        with open(self.json_file, "w", encoding=ENCODING) as f:
            json.dump(content, f)

    def default_json(self):
        """Set Json file to default."""
        with open(self.json_file, "w", encoding=ENCODING) as f:
            json.dump(self.json_default, f)

    def parse_json(self) -> Dict:
        """Parse Json to get content."""
        with open(self.json_file, "r", encoding=ENCODING) as f:
            js_dict = json.load(f)
        return js_dict

    def set_empty_json_to_default(self):
        """Set an empty Json to default."""
        if os.stat(self.json_file).st_size == 0:
            with open(self.json_file, "w+", encoding=ENCODING) as f:
                json.dump(self.json_default, f)

    @staticmethod
    def find_json_file(files, json_name):
        """Find json file among config files."""
        found = 0
        out = ""
        for f in files:
            # more than 1 found
            if found > 1:
                raise FileExistsError(
                    f"""More than one file names {json_name} under config directory.
                    Remove duplicated one."""
                )
            # find json file name in file path
            if json_name in f:
                found += 1
                out = f
                break

        # If fails to find token file
        if not found:
            raise FileNotFoundError(
                f"json file {json_name} doesn't exist under config directory."
            )

        return out


class ConfigPath:
    """Configuration Path."""

    def __init__(self) -> None:
        """Initialize configuration instance."""
        self.app_name = "GatorTracer"
        # author_app equals to app_name if not specified
        self.config_dir = user_config_dir(self.app_name)
        # key -> dirs, value -> list of config files
        # file -> string, folder -> sub_dict
        self.config_files_tree: Dict[List[str], str] = {
            "fetch_scope": ["exclude.json", "include.json"],
            "secret": ["token.txt"],
        }

        self.config_files = ConfigPath.parse_path_dict(self.config_files_tree)

    def initialize_config_path(self):
        """Initialize path of all needed user configuration paths."""
        main_config_path = Path(self.config_dir)

        # make directory of config path
        if not main_config_path.is_dir():
            main_config_path.mkdir(parents=True)
            print(f"📁 Initializing user config directory {main_config_path}")

        # make sub files in the sub folders from main config path
        for file in self.config_files:
            file_path = main_config_path / file

            # make all the parent dirs from the file
            file_path.parents[0].mkdir(parents=True, exist_ok=True)

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
                if isinstance(content, dict):
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
                    raise TypeError(
                        f"""Unexpected type {type(content)} for {content} in the path tree.
                         Should be str for file or dict for dir."""
                    )
        return parsed_paths
