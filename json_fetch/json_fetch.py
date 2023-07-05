import base64
import copy
import json
import re
from typing import List, Tuple
from pathlib import Path
from config_console import Token
from github import Github, Organization
import pandas as pd

# dev
import numpy as np
# TODO: add error msg and passing msg


class JsonFetch:
    def __init__(self, token: str, instructions: Tuple) -> None:
        self.authenticated_api = Github(token)
        (
            self.included_orgs,
            self.included_repos,
            self.excluded_orgs,
            self.excluded_repos,
        ) = instructions
        self.out_dict = {"organizations": []}

    def get_insight_jsons(self, dir, branch, file_regex):
        """Find all the matching repos and orgs."""
        self.dir, self.branch, self.file_regex = dir, branch, file_regex
        self.find_matching_orgs()
        return TreeDict(self.out_dict)

    def find_matching_orgs(self):
        """Find all the matching organizations based on the inclusion/exclusion instruction and call finding_matching_repos."""
        # Combine a list of regular expressions with OR gate
        # If actual expression matches with any of expected regular expressions, then check should pass
        included_combined = "(" + ")|(".join(self.included_orgs) + ")"
        excluded_combined = "(" + ")|(".join(self.excluded_orgs) + ")"

        # If included orgs are specified, then only fetch the orgs matching with included orgs
        # Otherwise fetch all the orgs not matching with the excluded orgs
        if self.included_orgs:
            # DOCUMENTATION: use organization login other than name (i.e. use the url org name)
            # DOCUMENTATION: students have to be at least member to make the organization listed here, remind faculty to do so
            for org in self.authenticated_api.get_user().get_orgs():
                if org.login and re.match(included_combined, org.login):
                    self.out_dict["organizations"].append(
                        {
                            "org-name": org.login,
                            "repositories": self.find_matching_repos(org),
                        }
                    )
        else:
            for org in self.authenticated_api.get_user().get_orgs():
                if org.login and not re.match(excluded_combined, org.login):
                    self.out_dict["organizations"].append(
                        {
                            "org-name": org.login,
                            "repositories": self.find_matching_repos(org),
                        }
                    )
        return True

    def find_matching_repos(self, org_obj):
        """Find all the matching repositories in a organization based on the inclusion/exclusion instruction and put them into dictionary."""
        # Combine a list of regular expressions with OR gate
        # If actual expression matches with any of expected regular expressions, then check should pass
        included_combined = "(" + ")|(".join(self.included_repos) + ")"
        excluded_combined = "(" + ")|(".join(self.excluded_repos) + ")"

        # If included repos are specified, then only fetch the repos matching with included repos
        # Otherwise fetch all the repos not matching with the excluded repos
        matching_repos = []
        if self.included_repos:
            for repo in org_obj.get_repos():
                if re.match(included_combined, repo.name):
                    matching_repos.append(
                        {
                            "repo-name": repo.name,
                            "insights": self.find_matching_files(repo),
                        }
                    )

        else:
            for repo in org_obj.get_repos():
                if not re.match(excluded_combined, repo.name):
                    matching_repos.append(
                        {
                            "repo-name": repo.name,
                            "insights": self.find_matching_files(repo),
                        }
                    )
        return matching_repos

    def find_matching_files(self, repo_obj):
        """Fetch all the immediate json files in a directory."""
        files_dict = []
        for f in repo_obj.get_contents(self.dir, ref=self.branch):
            if (
                f.type == "file"
                and f.name.endswith(".json")
                and re.match(self.file_regex, f.name)
            ):
                decoded_content = base64.b64decode(f.content).decode("utf-8")
                # get file name without extension
                f_pure_name = ".".join(f.name.split(".")[:-1])
                files_dict.append({"file-name": f_pure_name, "insight": decoded_content})
        return files_dict

    def fetch_latest_json(self, dir):
        """Fetch the latest generated json associated with its name."""
        # TODO: should only support the files generated in the format of date
        # TODO: make it a plugin
        pass


class TreeDict:
    """A nested dictionary."""

    def __init__(self, nested_dict) -> None:
        """Create TreeDict instance with a dictionary"""
        self.__nested_dict = nested_dict

    def __str__(self):
        """return the nested_dictionary"""
        return str(self.__nested_dict)

    def to_flatten_matrix(self, parsing_insight:bool = False):
        file_lvl_matrix = self.flatten_matrix_without_parsing_insight_json()
        if not parsing_insight:
            return file_lvl_matrix
        else:
            self.matrix_with_flatten_insight_json(file_lvl_matrix)

    def matrix_with_flatten_insight_json(self, matrix_with_header):
        main_df = pd.DataFrame(matrix_with_header[1:], columns=matrix_with_header[0])
        insights = main_df["insight"]
        print(insights)
        del main_df["insight"]
        for insight_idx in range(len(insights)):
            insight_str = insights[insight_idx]
            # load the content of insight in json format  
            insight_dict = json.loads(insight_str)
            insight_w_header = InsightReport(insight_dict).to_array_with_header()
            # Get all checks state
            insight_headers = insight_w_header[0]
            insight_values = insight_w_header[1]

            # Find matching headers in the main df
            for i in range(len(insight_headers)):
                insight_header =insight_headers[i]
                # If fails to find one then create an empty one column with the insight header
                if insight_header not in main_df.columns:
                    main_df[insight_header] = None

                main_df.loc[insight_idx,insight_header] = insight_values[i]
        
        main_df.to_csv("report.csv")

        



    def flatten_matrix_without_parsing_insight_json(self) -> List:
        """Flatten the nested dictionary based on the leaf and put into matrix."""
        rows = []
        title = []

        def flatten(d, values: List = []):
            # The keys whose value is a list of dictionary
            keys_to_list = []
            found_list = False
            for k in d:
                if isinstance(d[k], str) or isinstance(d[k], int):
                    values.append(d[k])
                    title.append(k) if k not in title else None
                elif isinstance(d[k], list) and all(
                    isinstance(sub, dict) for sub in d[k]
                ):
                    keys_to_list.append(k)
                    found_list = True
                else:
                    pass  # TODO: currently assume there is no other types
            # Base case: there is no more sub-dictionary
            if not found_list:
                v = copy.deepcopy(values)
                # Put all the static values like string, integer (non list nor dict) of ancestors (include the current node) into the rows
                rows.append(v)
            # General case: call flatten function recursively with sub-dictionary as root
            else:
                for k in keys_to_list:
                    for sub_d in d[k]:
                        v = copy.deepcopy(values)
                        flatten(sub_d, v)

        flatten(self.__nested_dict)
        matrix_with_title = [title] + rows
        return matrix_with_title


class InsightReport:
    def __init__(self, insight_dict) -> None:
        self.insight_dict = insight_dict
    
    def to_array_with_header(self):
        array_w_header = [[],[]]
        
        # Add percentage_score and amount_correct

        array_w_header[0].extend(["passing_percentage","amount_correct"])
        # Convert a percentage score string to a decimal percentage ex: "53" to 0.53
        passing_percentage = "{0:.2f}".format(int(self.insight_dict["percentage_score"])*0.01)
        array_w_header[1].append(passing_percentage) if "percentage_score" in self.insight_dict else array_w_header[1].append(None)
        array_w_header[1].append(int(self.insight_dict["amount_correct"])) if "amount_correct" in self.insight_dict else array_w_header[1].append(None)

        checks = dict()
        for check in self.insight_dict["checks"]:
            if "check" in check:
                check_type = check["check"]
            elif "command" in check:
                # TODO: doesn't support command now as the wide variety of the command types like lint, running correctly and etc
                # TODO: maybe we should add tag to the command to standardize them
                pass
            else:
                pass

            # distinguish the checks with different fragments. e.x.: MatchFileFragment_TODO isn't the same with MatchFileFragment_NAME
            if "options" in check and "fragment" in check["options"]:
                check_type +=  "_" + str(check["options"]["fragment"])
            
            # implement AND logic. If multi checks have the same check type. Pass True iff all the status is True
            if check_type not in checks:
                checks[check_type] = check["status"]
            
            # as long as one of the checks with the same type is False, then the check type should be False
            elif checks[check_type] == True and check["status"] == False:
                checks[check_type] = False
            else:
                # Don't change anything in other cases
                pass

        header_bool_pair = list(checks.items())
        for pair in header_bool_pair:
            header = pair[0]
            pass_state = pair[1]
            array_w_header[0].append(header)
            array_w_header[1].append(pass_state)

        return array_w_header


if __name__ == "__main__":
    with open("json_fetch/config/out.json","r") as js:
        json_report = json.load(js)
    print(type(json_report))
    insight_report = TreeDict(json_report)
    insight_report.to_flatten_matrix(parsing_insight=True)
