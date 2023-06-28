import base64
import json
import re
from typing import Tuple

from github import Github, Organization

from gh_token import Token


class Reference:
    # TODO: it's not necessary to have a storage file system to store those instructions as users may have different requirements every time they use this program
    def __init__(self) -> None:
        with open("data/include.json", "r") as js:
            self.in_json = json.load(js)

        self.in_repos = self.in_json["repository"]
        self.in_orgs = self.in_json["organization"]
        # self.ex_repo
        # self.ex_org


class JsonFetch:
    def __init__(self, token, instructions: Tuple) -> None:
        self.authenticated_api = Github(token)
        (
            self.included_orgs,
            self.included_repos,
            self.excluded_orgs,
            self.excluded_repos,
        ) = instructions
        self.out_dict = {"organizations":[]}

    def get_insight_jsons(self, dir, branch, file_regex="."):
        """Find all the matching repos and orgs."""
        self.dir, self.branch, self.file_regex = dir, branch, file_regex
        self.find_matching_orgs()
        return True

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
                    self.out_dict["organizations"].append({"org-name": org.login,"repositories":self.find_matching_repos(org)})
        else:
            for org in self.authenticated_api.get_user().get_orgs():
                if org.login and not re.match(excluded_combined, org.login):
                    self.out_dict["organizations"].append({"org-name": org.login,"repositories":self.find_matching_repos(org)})
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
                    matching_repos.append({"repo-name": repo.name, "insights":self.find_matching_files(repo)})

        else:
            for repo in org_obj.get_repos():
                if not re.match(excluded_combined, repo.name):
                    matching_repos.append({"repo-name": repo.name, "insights":self.find_matching_files(repo)})
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
                files_dict.append({"file":f_pure_name, "json":json.loads(decoded_content)})
        return files_dict

    def fetch_latest_json(self, dir):
        """Fetch the latest generated json associated with its name."""
        # TODO: should only support the files generated in the format of date
        # TODO: make it a plugin
        pass
    

if __name__ == "__main__":
    refence = Reference()
    token_obj = Token()
    token = token_obj.get_token()
    json_fetch = JsonFetch(
        token, instructions=(refence.in_orgs, refence.in_repos, [], [])
    )
    json_fetch.get_insight_jsons("insight","insight",file_regex="insight+.")
    with open("data/out.json", "w") as out:
        json.dump(json_fetch.out_dict, out, indent=4)
