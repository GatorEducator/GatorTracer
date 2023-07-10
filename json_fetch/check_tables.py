"""Generate tables for each type of check plus a main table."""
import base64
import hashlib
import json
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple, Type, Union

import polars as pl
import rich

CHECK_KEY = "check"
COMMAND_KEY = "command"
CHECKS_LIST_KEY = "checks"
UID_VAR = "uid"
MAIN_TABLE_NAME = "MainTable"
DTYPE_REF = {"str": pl.Utf8, "int": pl.Int64, "float": pl.Float64}


# TODO: Make a parent Table for check table and Main table
class MainTable:
    def __init__(self, table_dir: Path) -> None:
        self.main_table_path = table_dir / f"{MAIN_TABLE_NAME}.csv"
        self.table_place_holder = "deleteme"
        self.df = (
            pl.read_csv(self.main_table_path)
            if self.main_table_path.is_file()
            else pl.DataFrame()
        )

    def update(self, new_df: pl.DataFrame):
        """Update the main dataframe with a new dataframe."""
        # \n blank line breaks csv file, replace it with \t
        new_df = new_df.with_columns(pl.col(pl.Utf8).str.replace_all("\n", "\t"))
        self.df = pl.concat([self.df, new_df], how="diagonal").unique(subset=[UID_VAR])
        self.df.write_csv(self.main_table_path)
        return self

    def get_reports_by_uids(self, uids: List[str]):
        """return a list of insight rows by uids."""
        matchings = pl.DataFrame()
        for uid in uids:
            matchings = pl.concat([self.df.filter(pl.col(UID_VAR == uid)), matchings])

        return matchings


class CheckTable:
    def __init__(self, table_dir: Path, check_type: str) -> None:
        self.check_table_path = (table_dir) / f"{check_type}.csv"
        self.df = (
            pl.read_csv(self.check_table_path)
            if self.check_table_path.is_file()
            else pl.DataFrame()
        )

    def update(self, new_df):
        """Update the main dataframe with a new dataframe."""
        self.df = pl.concat([self.df, new_df], how="diagonal").unique()
        self.df.write_csv(self.check_table_path)
        return self

    def select_checks_by_uid(self, uid):
        df_fits_uid = self.df.filter(pl.col(UID_VAR) == uid)
        return df_fits_uid

    def get_checks_by_uids(self, uids: List[str]):
        """return a list of insight rows by uids."""
        matchings = pl.DataFrame()
        for uid in uids:
            matchings = pl.concat([self.df.filter(pl.col(UID_VAR == uid)), matchings])

        return matchings


class TableManager:
    def __init__(self, table_path: str) -> None:
        self.table_path = Path(table_path)
        self.checks_dir = self.table_path / Path("CheckTables")
        self.initialize_table_path()
        self.tables: Union[
            MainTable, CheckTable
        ] = TableManagerHelper.load_existing_tables(self.table_path)

    def initialize_table_path(self):
        if self.checks_dir.is_dir():
            pass
        else:
            self.checks_dir.mkdir(parents=True)

    def append_table_from_matrix(self, observations_w_header: pl.DataFrame):
        """Append items into the target tables based on a matrix where insights are not parsed yet."""
        print("🚀 Adding new matrix to tables....")
        row_amount = observations_w_header.height
        # Fetch all the insights and drop them from dataframe
        insights = observations_w_header["insight"]
        observations_without_insight = observations_w_header.drop(["insight"])
        # iterate over each insight row as a single observation
        for row_idx in range(row_amount):
            # Fetch all the variables of one row without insight
            # exclude index in row when generating uid as index misleads
            row_variables = (
                observations_without_insight.drop([""]).row(row_idx)
                if "" in observations_without_insight.columns
                else observations_without_insight.row(row_idx)
            )
            # Generate an unique string based on the information of one row
            # To generate an uid
            row_unique_string = " ".join(str(item) for item in row_variables)
            uid = TableManagerHelper.generate_uid(row_unique_string)

            # Store the uid in the main table
            observations_without_insight = TableManagerHelper.update_value_in_df(
                observations_without_insight, UID_VAR, row_idx, type(uid), uid
            )

            insight_dict = json.loads(insights[row_idx])
            insight_metadata, insight_checks = TableManagerHelper.triage_checks(
                insight_dict
            )

            # embed the insight metadata to the main dataframe
            for insight_info in insight_metadata:
                inf_value = insight_metadata[insight_info]
                observations_without_insight = TableManagerHelper.update_value_in_df(
                    observations_without_insight,
                    insight_info,
                    row_idx,
                    type(inf_value),
                    inf_value,
                )
            # Append the check dicts of insight to name-based check tables
            for check_type in insight_checks:
                ct = CheckTable(self.checks_dir, check_type)
                # Record CheckTable instance
                self.tables[check_type] = ct
                for one_check in insight_checks[check_type]:
                    # replace all the line breaks
                    for k in one_check:
                        one_check[k] = (
                            one_check[k].replace("\n", "\t")
                            if isinstance(one_check[k], str)
                            else one_check[k]
                        )

                    # Add insight uid to the check
                    one_check[UID_VAR] = uid
                    ct.update(pl.DataFrame(one_check))
        rich.print("MainTable: \n")
        print(observations_without_insight)
        mt = MainTable(self.table_path)
        self.tables[MAIN_TABLE_NAME] = mt
        mt.update(observations_without_insight)
        rich.print(f"[green] successfully updated or generate all the tables under path: {self.table_path}.")

    def select_checks_by_uid(self, uid: str, save_csv: str = "") -> pl.DataFrame:
        """Select all the checks sharing the same uid."""
        found_checks_df = pl.DataFrame()
        for check_table in self.checks_dir.iterdir():
            if check_table.is_file() and check_table.suffix == ".csv":
                # check_table.stem is the file name without extension
                ct = CheckTable(self.checks_dir, check_table.stem)
                check_df = ct.select_checks_by_uid(uid)
                found_checks_df = pl.concat(
                    [found_checks_df, check_df], how="diagonal"
                ).unique()
        print(found_checks_df)
        if save_csv:
            found_checks_df.write_csv(save_csv)
            rich.print(f"[green] csv file has been saved in {save_csv}")
        return found_checks_df

    def get_checks_by_attribute_one_table(
        self, attribute: str, attribute_value, with_report=False, table="MainTable"
    ):
        """Get matching checks in a specific table."""
        if not isinstance(attribute, str):
            raise TypeError(f"Column name only accepts string type")

        if table == MAIN_TABLE_NAME:
            mt = self.tables[table]
            df: pl.DataFrame = mt.df
        else:
            check_csv = self.checks_dir / f"{table}.csv"
            # Check not found
            if not check_csv.is_file():
                raise ValueError(f"No such a check table called {table}")
            ct = self.tables[table]
            df: pl.DataFrame = ct.df

        # Skip if column name not found to escape ColumnNotFoundError
        if attribute not in df.columns:
            return pl.DataFrame()

        # If column value is a string and the string is not purely numeric
        if isinstance(attribute_value, str) and not attribute_value.isnumeric():
            # Then match with regex
            matching_rows = df.filter(pl.col(attribute).str.contains(attribute_value))

        # If column value is a numeric in string type
        elif isinstance(attribute_value, str) and attribute_value.isnumeric():
            # Then Try best to convert data type to float
            attribute_value = float(attribute_value)
            # If the column is in the integer pattern e.x.: 5.00
            # Then convert it to integer
            if attribute_value.is_integer():
                attribute_value = int(attribute_value)
            matching_rows = df.filter(pl.col(attribute) == attribute_value)
        else:
            matching_rows = df.filter(pl.col(attribute) == attribute_value)

        # Concatenate the row of insight report with check rows
        if with_report:
            # Use uid to find all the report rows in main table
            main_table: pl.DataFrame = self.tables[MAIN_TABLE_NAME].df

            # Drop the index column if exists one
            main_table = main_table.drop("") if "" in main_table.columns else main_table
            uids = matching_rows[UID_VAR].to_list()
            reports = pl.DataFrame()
            # Find the matching rows associated with uid
            for uid in uids:
                row_matching_uid = main_table.filter(pl.col(UID_VAR) == uid)
                # Matching df should be one row, as uid is unique each row
                if row_matching_uid.height != 1:
                    raise ValueError(
                        "more than one reports share the same unique identifier!"
                    )

                # record all the reports
                # uid exists both main table and check table, drop one to avoid conflict
                reports = pl.concat(
                    [reports, row_matching_uid.drop(UID_VAR)], how="vertical"
                )

            # uid column exists both dataframes
            matching_rows_w_reports = pl.concat(
                [matching_rows, reports], how="horizontal"
            )
            return matching_rows_w_reports
        return matching_rows

    def get_checks_by_attribute_across_tables(
        self, attribute: str, attribute_value, with_report=False
    ):
        """Get matching checks across tables.

        Args:
            attribute: the attribute name. e.g.: status
            attribute_value: the selected attribute value associated with attribute. e.g.: True
            with_report: glue checks with its insight report file information
        """
        check_type_col_name = "check type"
        check_df = pl.DataFrame(
            {
                check_type_col_name: pl.Series([], dtype=pl.Utf8),
            }
        )
        for table_name in self.tables:
            checks_in_one_table = self.get_checks_by_attribute_one_table(
                attribute, attribute_value, with_report=with_report, table=table_name
            )
            # ignore empty dataframe
            if checks_in_one_table.is_empty():
                continue
            # Tag check type to the df generated from on table
            checks_in_one_table = checks_in_one_table.with_columns(
                pl.lit(table_name).alias(check_type_col_name).cast(pl.Utf8)
            )

            check_df = pl.concat([check_df, checks_in_one_table], how="diagonal")
        # all the matching checks across tables
        return check_df

    def get_table(self, table_name=MAIN_TABLE_NAME):
        """Get a table dataframe."""
        if table_name == MAIN_TABLE_NAME:
            return self.tables[table_name].df
        else:
            return self.tables[table_name].df


class TableManagerHelper:
    @staticmethod
    def generate_uid(info: str):
        """generate a string uid from a string."""
        # Hash the row string using MD5
        md5_hash = hashlib.md5(info.encode())

        # A message digest is a cryptographic hash function containing a string of digits
        digest = md5_hash.digest()

        encoded_id = base64.b64encode(digest)
        short_id = encoded_id.decode()
        return short_id

    @staticmethod
    def load_existing_tables(path: Path) -> List[Tuple[Path, str]]:
        """Load all the tables to a dictionary of dataframe under the current directory and sub-directories."""

        table_dir = {}
        # Create a Path object for the directory

        # Get a list of all files in the directory and its subdirectories
        files_and_dirs = list(path.glob("**/*"))
        # Filter out directories from the list
        # Get table files
        table_paths = [
            file for file in files_and_dirs if file.is_file() and file.suffix == ".csv"
        ]
        # Convert Path objects to string paths
        table_dir_file_pairs = [(file.parent, file.stem) for file in table_paths]

        for dir_file_pair in table_dir_file_pairs:
            dir_path = dir_file_pair[0]
            file_name = dir_file_pair[1]
            if file_name == MAIN_TABLE_NAME:
                table_dir[file_name] = MainTable(dir_path)
            else:
                table_dir[file_name] = CheckTable(dir_path, file_name)

        return table_dir

    @staticmethod
    def triage_checks(insight: Dict):
        def flatten_check(dic):
            """recursively fetch key none_dict pairs to an one-dimension dictionary."""
            flattened_pairs = {}
            for arg in dic:
                if isinstance(dic[arg], Dict):
                    sub_pairs = flatten_check(dic[arg])
                    flattened_pairs.update(sub_pairs)
                else:
                    flattened_pairs[arg] = dic[arg]

            return flattened_pairs

        file_level_inf = {}
        checks_dict = defaultdict(list)

        for arg in insight:
            # if not checks list key, then the item is file level other than check level
            # Fetch all the insight level information
            # Those inf will end up in the main table
            if arg != CHECKS_LIST_KEY:
                file_level_inf[arg] = insight[arg]

            # Fetch all the check level information
            # Those data will end up in the check sub table
            else:
                for check in insight[arg]:
                    flattened_check = flatten_check(check)
                    if CHECK_KEY in flattened_check:
                        # promote check type to higher level
                        check_type = flattened_check[CHECK_KEY]
                        flattened_check.pop(CHECK_KEY)
                        checks_dict[check_type].append(flattened_check)

                    elif COMMAND_KEY in flattened_check:
                        # promote check type to higher level
                        checks_dict[COMMAND_KEY.capitalize()].append(flattened_check)
        return file_level_inf, checks_dict

    @staticmethod
    def update_value_in_df(
        df: pl.DataFrame, column_name, row_idx, input_date_type: Type, new_value
    ) -> pl.DataFrame:
        """Update value in a df. If the column doesn't exist, automatically generate one."""
        # Find related polars data type from input_data_type
        pl_dtype = DTYPE_REF[input_date_type.__name__]
        # If no such an column exists in df, then generate a null column with this column
        if column_name not in df.columns:
            df = df.with_columns(pl.lit(None).alias(column_name).cast(pl_dtype))

        # Update the value of column in a specific row
        df[row_idx, column_name] = new_value
        return df


if __name__ == "__main__":
    # m = MainTable(".")

    df = pl.read_csv("report.csv")
    t = TableManager("tables")
    # a = t.append_table_from_matrix(df)
    a = t.get_table("MainTable")
    checks = t.get_checks_by_attribute_across_tables(
        attribute="objective", attribute_value="meet minimal", with_report=True
    )
    print(checks)
    checks.write_csv("wow.csv")
