"""Official CLI."""
import typer
from config_console import Token, ConfigJson
from typing_extensions import Annotated
from json_fetch import JsonFetch
from pathlib import Path
from pprintjson import pprintjson
import json
import pandas as pd

cli = typer.Typer()
EXCLUDED_JSON = Path(__file__).parent.resolve() / "config" / "exclude.json"
INCLUDED_JSON = Path(__file__).parent.resolve() / "config" / "include.json"


@cli.command()
def saved_token(
    verify: bool = typer.Option(
        False, "--verify", "-v", help="verify if there is stored gh token"
    ),
    save: str = typer.Option("", "--save", "-s", help="save a new gh token"),
    rm: bool = typer.Option(
        False, "--remove", "-r", help="remove the currente stored gh token"
    ),
):
    gh_token = Token()

    if rm:
        return_flag = gh_token.remove_token()
        if return_flag:
            print("Saved token has been removed")
        else:
            print("No saved token exists to be removed")
    if save:
        gh_token.set_token(save)
        print("Token has been saved")
    if verify:
        print(gh_token.token_exists())


@cli.command()
def config(
    display_all: bool = typer.Option(
        False, "--display-all", help="display both exclude and include config json file"
    ),
    display_in: bool = typer.Option(
        False, "--display-in", help="display the include config json file"
    ),
    display_ex: bool = typer.Option(
        False, "--display-ex", help="display the exclude config json file"
    ),
    clear_all: bool = typer.Option(
        False, "--clear-all", help="clear both exclude and include config json file"
    ),
    clear_in: bool = typer.Option(
        False, "--clear-in", help="clear include config json file"
    ),
    clear_ex: bool = typer.Option(
        False, "--clear-ex", help="clear exclude config json file"
    ),
    in_from_file: str = typer.Option(
        "",
        "--in-from-file",
        help="Write configuration include.json from another json file",
    ),
    ex_from_file: str = typer.Option(
        "",
        "--ex-from-file",
        help="Write configuration exclude.json from another json file",
    ),
):
    in_config = ConfigJson(INCLUDED_JSON)
    ex_config = ConfigJson(EXCLUDED_JSON)
    if display_all:
        print()
        print("include.json\n")
        pprintjson(in_config.parse_json())
        print("---------------\n")
        print("exclude.json\n")
        pprintjson(ex_config.parse_json())

    if display_in:
        print()
        print("include.json\n")
        pprintjson(in_config.parse_json())

    if display_ex:
        print()
        print("exclude.json\n")
        pprintjson(ex_config.parse_json())

    if clear_all:
        in_config.default_json({})
        ex_config.default_json({})

    if clear_in:
        # TODO: preserve key organization and repo
        in_config.default_json({})

    if clear_ex:
        ex_config.default_json({})

    if in_from_file:
        source_file = Path(in_from_file)
        if source_file.is_file() and in_from_file.endswith(".json"):
            with open(source_file, "r") as f:
                js = json.load(f)
            in_config.write_json(js)
        else:
            raise FileNotFoundError(f"Can'find json file {in_from_file}")

    if ex_from_file:
        source_file = Path(ex_from_file)
        if source_file.is_file() and ex_from_file.endswith(".json"):
            with open(source_file, "r") as f:
                js = json.load(f)
            ex_config.write_json(js)
        else:
            raise FileNotFoundError(f"Can'find json file {ex_from_file}")


@cli.command()
def js_fetch(
    token: str = typer.Option(
        ...,
        "--token",
        "-t",
        prompt="saved token or tmp token[S/T]",
        help="Choose the kind of token to use: either the saved token or the temporary token",
    ),
    branch: str = typer.Option(
        "insight", "--branch", "-b", help="The branch where json(s) reside"
    ),
    dir: str = typer.Option(
        ..., "--dir", "-d", help="The directory where json(s) reside"
    ),
    file_re: str = typer.Option(
        ".", "--file", "-f", help="The file names in the regex format"
    ),
    all_insights: bool = typer.Option(
        False, "--all", "-a", help="fetch all the json files in the target directory"
    ),
):
    print(file_re)
    token_value = ""
    while token not in "sStT":
        token = input("please select S (saved token) or T (temporary token): ")

    if token in "sS":
        saved_token = Token()
        if saved_token.token_exists():
            token_value = saved_token.get_token()
            print("successfully fetched saved token")
        else:
            raise ValueError(
                "No saved token, run subcommand `saved_token --save` to set up one or use temporary token."
            )
    else:
        token_value = input("Please provide a github token (it won't be saved): ")

    if all_insights:
        excluded_dict = ConfigJson(EXCLUDED_JSON).parse_json()
        included_dict = ConfigJson(INCLUDED_JSON).parse_json()
        excluded_org, excluded_repo = (
            excluded_dict["organization"],
            excluded_dict["repository"],
        )
        included_org, included_repo = (
            included_dict["organization"],
            included_dict["repository"],
        )
        print(included_org)
        ex_in = (included_org, included_repo, excluded_org, excluded_repo)
        json_fetch_handler = JsonFetch(token=token_value, instructions=ex_in)
        insight_tree = json_fetch_handler.get_insight_jsons(
            dir=dir, branch=branch, file_regex=file_re
        )
        insight_matrix = insight_tree.to_flatten_matrix()
        print(insight_matrix)
        df = pd.DataFrame(insight_matrix[1:], columns=insight_matrix[0])
        csv_format = df.to_csv("report.csv", header=True)
        print(df)


if __name__ == "__main__":
    cli()
