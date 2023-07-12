"""Fetch jsons from GitHub."""
import base64
import copy
import re
from typing import Dict, List, Pattern, Tuple

from github import Github, Organization, Repository


class JsonFetch:
    """Fetch Json."""

    # pylint: disable=too-many-instance-attributes
    # pylint: disable=attribute-defined-outside-init
    def __init__(self, token: str, instructions: Tuple) -> None:
        self.authenticated_api = Github(token)
        (
            self.included_orgs,
            self.included_repos,
            self.excluded_orgs,
            self.excluded_repos,
        ) = instructions
        self.out_dict = {"organizations": []}

    def get_insight_jsons(self, directory: str, branch: str, file_regex: Pattern[str]):
        """Find all the matching repos and orgs."""
        self.directory, self.branch, self.file_regex = directory, branch, file_regex
        self.find_matching_orgs()
        return TreeDict(self.out_dict)

    def find_matching_orgs(self):
        """Find all the matching organizations based on the inclusion/exclusion instruction
        and call finding_matching_repos."""
        # Combine a list of regular expressions with OR gate
        # If actual expression matches with any of expected regular expressions, check should pass
        included_combined = "(" + ")|(".join(self.included_orgs) + ")"
        excluded_combined = "(" + ")|(".join(self.excluded_orgs) + ")"

        # If included orgs are specified, then only fetch the orgs matching with included orgs
        # Otherwise fetch all the orgs not matching with the excluded orgs
        if self.included_orgs:
            # use organization login other than name (i.e. use the url org name)
            # students have to be at least member in the organization
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

    def find_matching_repos(self, org_obj: Organization.Organization) -> List[Dict]:
        """Find all the matching repositories in a organization
        based on the inclusion/exclusion instruction and put them into dictionary."""
        # Combine a list of regular expressions with OR gate
        # If actual expression matches with any of expected regular expressions, check should pass
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

    def find_matching_files(self, repo_obj: Repository.Repository) -> List[Dict]:
        """Fetch all the immediate json files in a directory."""
        # pylint: disable = invalid-name
        files_dict = []
        for f in repo_obj.get_contents(self.directory, ref=self.branch):
            if (
                f.type == "file"
                and f.name.endswith(".json")
                and re.match(self.file_regex, f.name)
            ):
                decoded_content = base64.b64decode(f.content).decode("utf-8")
                # get file name without extension
                f_pure_name = ".".join(f.name.split(".")[:-1])
                files_dict.append(
                    {"file-name": f_pure_name, "insight": decoded_content}
                )
        return files_dict


class TreeDict:
    """A nested dictionary."""

    def __init__(self, nested_dict: Dict) -> None:
        """Create TreeDict instance with a dictionary."""
        self.__nested_dict = nested_dict

    def __str__(self):
        """return the nested_dictionary"""
        return str(self.__nested_dict)

    def to_flatten_matrix(self) -> List[List]:
        """Flatten the nested dictionary based on the leaf and put into matrix."""
        print("🚀 Converting nested dictionary to a flat matrix.")
        rows = []
        title = []

        def flatten(root_dict, values):
            """Recursively find the non-iterable and put into matrix.

            Args:
                root_dict: dictionary
                values: a list of recorded non-iterable values inherited from parent dictionary
            """
            # The keys whose value is a list of dictionary
            keys_to_list = []
            found_list = False
            for k in root_dict:
                if isinstance(root_dict[k], (str, int)):
                    values.append(root_dict[k])
                    if k not in title:
                        title.append(k)
                elif isinstance(root_dict[k], list) and all(
                    isinstance(sub, dict) for sub in root_dict[k]
                ):
                    keys_to_list.append(k)
                    found_list = True
                else:
                    pass  # currently assume there is no other types
            # Base case: there is no more sub-dictionary
            if not found_list:
                # Python pass argument by reference.
                # Call with descend of dictionary will change values of variable
                # Then next run of recursive function will have different content in the variable
                # So deepcopy to avoid the rewriting from this feature
                value = copy.deepcopy(values)
                # Put all the non-iterable values (e.g.: int, str) into the rows
                rows.append(value)
            # General case: call flatten function recursively with sub-dictionary as root
            else:
                for k in keys_to_list:
                    for sub_d in root_dict[k]:
                        value = copy.deepcopy(values)
                        flatten(sub_d, value)

        flatten(self.__nested_dict, [])
        matrix_with_title = [title] + rows
        print("⭐ flat matrix was built successfully")
        return matrix_with_title
