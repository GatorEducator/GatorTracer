from github import Github
import json
import re
import base64

OUT_DICT = {}
REF = "main"
PATH = "README.md"


class Reference:
    def __init__(self) -> None:
        with open("data/include.json", "r") as js:
            self.in_json = json.load(js)

        self.in_repo = self.in_json["repository"]
        self.in_org = self.in_json["organization"]
        # self.ex_repo
        # self.ex_org


class Token:
    def __init__(self) -> None:
        self.__token_file = "data/token.txt"  # TODO: file path should change as program running as pypi project
        # TODO: or maybe environment variable is better?
        self.__token = open(self.__token_file, "r").read()

    def token_exists(self):
        if_token_exists = self.__token != ""
        print(if_token_exists)
        return if_token_exists

    def set_token(self, token):
        with open(self.__token_file, "w") as f:
            f.write(token)
        self.__token = token

    def remove_token(self):
        with open(self.__token_file, "w") as f:
            f.write("")
        self.__token = ""

    # Probably shouldn't allow users to call this function??
    def get_token(self):
        return self.__token


def find_matching_repos(org):
    ref = Reference()
    for repo in org.get_repos():
        if re.match(ref.in_repo[0], repo.name):
            try:
                content = repo.get_contents(path=PATH, ref=REF)
                decoded = base64.b64decode(content.content).decode("utf-8")
                OUT_DICT[org.name][repo.name] = decoded
            except:
                pass


def find_matching_orgs():
    ref = Reference()
    token = Token()
    for org in Github(token.get_token()).get_user().get_orgs():
        if org.name and re.match(ref.in_org[0], org.name):
            OUT_DICT[org.name] = {}
            find_matching_repos(org)


if __name__ == "__main__":
    find_matching_orgs()
    with open("data/out.json", "w") as out:
        json.dump(OUT_DICT, out, indent=4)
