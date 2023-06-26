"""Token"""


class Token:
    def __init__(self) -> None:
        self.__token_file = "data/token.txt"  # TODO: file path should change as program running as pypi project
        # TODO: or maybe environment variable is better?
        self.__token = open(self.__token_file, "r").read()

    def token_exists(self):
        if_token_exists = self.__token != ""
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
