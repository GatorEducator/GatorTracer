"""Generate tables for each type of check plus a main table."""
import polars as pl
import json
import hashlib
import base64
from typing import List, Dict, Union, Type
from collections import defaultdict
import rich
from pathlib import Path
CHECK_KEY = "check"
COMMAND_KEY = "command"
CHECKS_LIST_KEY = "checks"
UID_VAR = "uid"
DTYPE_REF = {"str": pl.Utf8,
"int": pl.Int64,
"float": pl.Float64}


# TODO: Make a parent Table for check table and Main table
class MainTable:
    def __init__(self, table_dir: Path) -> None:
        self.main_table_path = table_dir / "MainTable.csv"
        self.table_place_holder = "deleteme"
        self.main_table =  pl.read_csv(self.main_table_path) if self.main_table_path.is_file() else pl.DataFrame()

    def update(self, new_df: pl.DataFrame):
        """Update the main dataframe with a new dataframe."""
        # \n blank line breaks csv file, replace it with \t
        new_df = new_df.with_columns(pl.col(pl.Utf8).str.replace_all("\n", "\t"))
        self.main_table = pl.concat([self.main_table,new_df], how= "diagonal").unique(subset=[UID_VAR])
        self.main_table.write_csv(self.main_table_path)
        return self

class CheckTable:
    def __init__(self, table_dir: Path, check_type: str) -> None:
        self.check_table_path = (table_dir) / f"{check_type}.csv"
        self.check_table =  pl.read_csv(self.check_table_path) if self.check_table_path.is_file() else pl.DataFrame()

    def update(self, new_df):
        """Update the main dataframe with a new dataframe."""
        self.check_table = pl.concat([self.check_table,new_df], how= "diagonal").unique()
        self.check_table.write_csv(self.check_table_path)
        return self

    def select_checks_by_uid(self,uid):
        df_fits_uid = self.check_table.filter(pl.col(UID_VAR) == uid)
        return df_fits_uid

class TableManager:
    def __init__(self, table_path:str) -> None:
        self.table_path = Path(table_path)
        self.checks_dir = self.table_path / Path("CheckTables")
        self.initialize_table_path()

    def initialize_table_path(self):
        if self.checks_dir.is_dir():
            pass
        else:
            self.checks_dir.mkdir(parents=True)

    def append_table_from_matrix(self, observations_w_header: pl.DataFrame):
        """Append items into the target tables based on a matrix where insights are not parsed yet."""

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
            insight_metadata, insight_checks = TableManagerHelper.triage_checks(insight_dict)

            # embed the insight metadata to the main dataframe
            for insight_info in insight_metadata:
                inf_value = insight_metadata[insight_info]
                observations_without_insight = TableManagerHelper.update_value_in_df(
                    observations_without_insight,
                    insight_info,
                    row_idx,
                    type(insight_info),
                    inf_value,
                )
            # Append the check dicts of insight to name-based check tables
            for check_type in insight_checks:
                ct = CheckTable(self.checks_dir, check_type)
                for one_check in insight_checks[check_type]:
                    # replace all the line breaks
                    for k in one_check:
                        one_check[k] = one_check[k].replace("\n","\t") if isinstance(one_check[k],str) else one_check[k]

                    # Add insight uid to the check
                    one_check[UID_VAR] = uid
                    ct.update(pl.DataFrame(one_check)) 
        rich.print("MainTable: \n")
        print(observations_without_insight)
        mt = MainTable(self.table_path)
        mt.update(observations_without_insight)
        rich.print("[green] successfully generates all the tables.")
    
    def select_checks_by_uid(self, uid:str, save_csv:str = "")-> pl.DataFrame:
        """Select all the checks sharing the same uid."""
        found_checks_df = pl.DataFrame()
        for check_table in self.checks_dir.iterdir():
            if check_table.is_file() and check_table.suffix == ".csv":
                # check_table.stem is the file name without extension
                ct = CheckTable(self.checks_dir,check_table.stem)
                check_df = ct.select_checks_by_uid(uid)
                found_checks_df = pl.concat([found_checks_df, check_df],how="diagonal").unique()
        print(found_checks_df)
        if save_csv:
            found_checks_df.write_csv(save_csv) 
            rich.print(f"[green] csv file has been saved in {save_csv}")
        return found_checks_df

    def get_uids_by_column(self, column_name:str, column_value, table = "MainTable"):
        """fetch a list of uids based on column name"""
        # TODO: get uids across tables
        if not isinstance(column_name,str):
            raise TypeError(f"Column name only accepts string type")

        if table == "MainTable":
            mt = MainTable(self.table_path)
            df = mt.main_table
        else:
            check_csv = self.checks_dir / f"{table}.csv"
            # Check not found
            if not check_csv.is_file():
                raise ValueError(f"No such a check table called {table}")
            ct = CheckTable(self.checks_dir, table)
            df = ct.check_table
        
        # If column value is a string and the string is not purely numeric
        if isinstance(column_value, str) and not column_value.isnumeric():
            # Then match with regex
            matching_rows = df.filter(pl.col(column_name).str.contains(column_value))


        # If column value is a numeric in string type
        elif isinstance(column_value, str) and column_value.isnumeric():
            # Then Try best to convert data type to float
            column_value = float(column_value)
            # If the column is in the integer pattern e.x.: 5.00
            # Then convert it to integer
            if column_value.is_integer():
                column_value = int(column_value)
            matching_rows = df.filter(pl.col(column_name)==column_value)
        
        else:
            matching_rows = df.filter(pl.col(column_name)==column_value)

        uids = matching_rows[UID_VAR].to_list()
        # Remove repeat uids
        unique_uids = list(set(uids))
        return unique_uids

    def get_table(self, table_name = "MainTable"):
        """Get a table dataframe."""
        if table_name == "MainTable":
            return MainTable(self.table_path).main_table
        else:
            return CheckTable(self.checks_dir,table_name).check_table

    
    def get_insight_row_w_uid(self):
        pass
class TableManagerHelper:
    @staticmethod
    def generate_uid( info: str):
        """generate a string uid from a string."""
        # Hash the row string using MD5
        md5_hash = hashlib.md5(info.encode())

        # A message digest is a cryptographic hash function containing a string of digits
        digest = md5_hash.digest()

        encoded_id = base64.b64encode(digest)
        short_id = encoded_id.decode()
        return short_id

    
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
        # If no such an column exists in df, then generate a null column with this column

        # Find related polars data type from input_data_type
        pl_dtype = DTYPE_REF[input_date_type.__name__]
        if column_name not in df.columns:
            df = df.with_columns(pl.lit(None).alias(column_name).cast(pl_dtype))

        # Update the value of column in a specific row
        df[row_idx, column_name] = new_value
        return df


if __name__ == "__main__":
    # m = MainTable(".")

    df = pl.read_csv("report.csv")
    t = TableManager("tables")
    t.append_table_from_matrix(df)
    # uids = t.get_uids_by_column(column_name="status", column_value=True, table="Command")
    # print(uids)
