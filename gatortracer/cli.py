"""Official CLI."""
# pylint: disable = too-many-arguments
# pylint: disable = invalid-name
import json
from pathlib import Path

import polars as pl
import typer
from pprintjson import pprintjson

from gatortracer.check_tables import TableManager
from gatortracer.config_console import *
from gatortracer.json_fetch import JsonFetch

cli = typer.Typer()
EXCLUDED_JSON, INCLUDED_JSON = "exclude.json", "include.json"


@cli.command()
def saved_token(
    verify: bool = typer.Option(
        False, "--verify", "-v", help="verify if there is stored gh token"
    ),
    save: str = typer.Option("", "--save", "-s", help="save a new gh token"),
    remove: bool = typer.Option(
        False, "--remove", "-r", help="remove the currente stored gh token"
    ),
):
    """CVUD (create, verify, update, delete) with saved token."""
    gh_token = Token()

    if remove:
        return_flag = gh_token.remove_token()
        if return_flag:
            print("Saved token has been removed")
        else:
            print("No saved token exists to be removed")
    if save:
        gh_token.set_token(save)
        print("Token has been saved")
    if verify:
        if gh_token.token_exists():
            print("Saved token exists")
        else:
            print("No saved token exists")


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
    """CRUD (create, read, update, delete) configuration files (not including saved-token)."""
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
        in_config.default_json()
        ex_config.default_json()

    if clear_in:
        in_config.default_json()

    if clear_ex:
        ex_config.default_json()

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
    directory: str = typer.Option(
        ..., "--dir", "-d", help="The directory where json(s) reside"
    ),
    file_re: str = typer.Option(
        ".", "--file", "-f", help="The file names in the regex foremoveat"
    ),
    store_path: str = typer.Option(
        ".", "--store-path", "-s", help="The path where the output files will inhabit."
    ),
):
    """Fetch desired json files in GitHub associate with the flags and user configuration files."""
    token_value = ""
    while token not in "sStT":
        token = input("please select S (saved token) or T (temporary token): ")

    # saved token
    if token in "sS":
        saved_token = Token()
        if saved_token.token_exists():
            token_value = saved_token.get_token()
            print("successfully fetched saved token")
        else:
            raise ValueError(
                "No saved token, run subcommand `saved_token --save` to set up one or use temporary token."  # pylint: disable = line-too-long
            )
    # else temporary token
    else:
        token_value = input("Please provide a github token (it won't be saved): ")

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
    ex_in = (included_org, included_repo, excluded_org, excluded_repo)
    json_fetch_handler = JsonFetch(token=token_value, instructions=ex_in)
    insight_tree = json_fetch_handler.get_insight_jsons(
        directory=directory, branch=branch, file_regex=file_re
    )
    insight_matrix = insight_tree.to_flatten_matrix()
    df = pl.DataFrame(insight_matrix[1:], schema=insight_matrix[0])
    table_manager = TableManager(store_path)
    table_manager.append_table_from_matrix(df)


@cli.command()
def select_checks(
    main_table_dir: str = typer.Option(
        ..., "--main-path", "-p", help="The directory where main table inhabit"
    ),
    attribute_name: str = typer.Option(
        ..., "--attribute", "-a", help="the attribute check selection is subject to"
    ),
    attribute_value: str = typer.Option(
        ..., "--value", "-v", help="the value associate with the attribute"
    ),
    save_file: str = typer.Option(
        "",
        "--save-file",
        "-s",
        help="if specified, then save output as csv in the path you choose",
    ),
    table_name: str = typer.Option(
        ".",
        "--table",
        "-t",
        help="""
        the table where you want to select checks from,
        all the available tables will be selected by default.
            """,
    ),
    with_report: bool = typer.Option(
        True,
        "--with-report",
        "-r",
        help="combine checks with report file inforemoveation",
    ),
):
    """Select checks."""
    table_manager = TableManager(main_table_dir)
    df = pl.DataFrame()
    # table name argument is set as default
    if table_name == ".":
        # Then find checks across all the tables
        df = table_manager.get_checks_by_attribute_across_tables(
            attribute_name, attribute_value, with_report
        )
    # Otherwise only find table in the desired table
    else:
        df = table_manager.get_checks_by_attribute_one_table(
            attribute_name, attribute_value, with_report, table_name
        )

    print(df)
    if save_file:
        df.write_csv(save_file)
    return df


@cli.callback()
def initialize_app():
    """User who access to this app."""
    cfg_path = ConfigPath()
    cfg_path.initialize_config_path()


if __name__ == "__main__":
    cli()
