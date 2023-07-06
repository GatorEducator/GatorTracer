"""Generate tables for each type of check plus a main table."""
import polars as pl
import json
import hashlib
import base64
from typing import List, Dict
from collections import defaultdict
import pprint
from pathlib import Path
CHECK_KEY = "check"
COMMAND_KEY = "command"
CHECKS_LIST_KEY = "checks"
UID_VAR = "uid"


class MainTable:
    def __init__(self, table_dir: Path) -> None:
        self.main_table_path = table_dir / "MainTable.csv"
        self.table_place_holder = "deleteme"
        self.initialize_table()
        self.main_table =  pl.read_csv(self.main_table_path)

    def update(self, new_df: pl.DataFrame):
        """Update the main dataframe with a new dataframe."""
        # \n blank line breaks csv file, replace it with \t
        new_df = new_df.with_columns(pl.col(pl.Utf8).str.replace_all("\n", "\t"))
        # If place holder is in the current main_table file, then replace the current table with the new table
        if self.table_place_holder in self.main_table.columns:
            self.main_table = new_df
        else:
            self.main_table = pl.concat([self.main_table,new_df], how= "diagonal").unique(subset=[UID_VAR])
        self.main_table.write_csv(self.main_table_path)
        return self
    
    def initialize_table(self):
        """initialize a main table csv file if not exists one."""
        if not self.main_table_path.is_file():
            with open(self.main_table_path,"w") as t:
                t.write(self.table_place_holder)


class CheckTable:
    def __init__(self, table_dir: Path, check_type: str) -> None:
        self.check_table_path = (table_dir) / f"{check_type}.csv"
        self.table_place_holder = "deleteme"
        self.initialize_table()
        self.main_table =  pl.read_csv(self.check_table_path)
    
    def initialize_table(self):
        """initialize a check table csv file if not exists one."""
        if not self.check_table_path.is_file():
            with open(self.check_table_path,"w") as t:
                t.write(self.table_place_holder)

    def update(self, new_df):
        """Update the main dataframe with a new dataframe."""
        # If place holder is in the current check_table file, then replace the current table with the new table
        if self.table_place_holder in self.main_table.columns:
            self.main_table = new_df
        else:
            self.main_table = pl.concat([self.main_table,new_df], how= "diagonal").unique()
        self.main_table.write_csv(self.check_table_path)
        return self

class CommandTable:
    def __init__(self) -> None:
        pass


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
            uid = self.generate_uid(row_unique_string)

            # Store the uid in the main table
            observations_without_insight = self.update_value_in_df(
                observations_without_insight, UID_VAR, row_idx, pl.Utf8, uid
            )

            insight_dict = json.loads(insights[row_idx])
            insight_metadata, insight_checks = self.triage_checks(insight_dict)

            # embed the insight metadata to the main dataframe
            for insight_info in insight_metadata:
                inf_value = insight_metadata[insight_info]
                observations_without_insight = self.update_value_in_df(
                    observations_without_insight,
                    insight_info,
                    row_idx,
                    pl.Int64,
                    inf_value,
                )
            # Append the check dicts to name-based check tables
            for check_type in insight_checks:
                ct = CheckTable(self.checks_dir, check_type)
                for one_check in insight_checks[check_type]:
                    # replace all the line breaks
                    for k in one_check:
                        one_check[k] = one_check[k].replace("\n","\t") if isinstance(one_check[k],str) else one_check[k]

                    # Add insight uid to the check
                    one_check[UID_VAR] = uid
                    ct.update(pl.DataFrame(one_check)) 

        print(observations_without_insight)
        mt = MainTable(self.table_path)
        mt.update(observations_without_insight)
        # TODO: parse insight_checks to the sub tables tmr

    def triage_checks(self, insight: Dict):
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
                        checks_dict[COMMAND_KEY.upper()].append(flattened_check)
        return file_level_inf, checks_dict

    def generate_uid(self, info: str):
        """generate a string uid from a string."""
        # Hash the row string using MD5
        md5_hash = hashlib.md5(info.encode())

        # A message digest is a cryptographic hash function containing a string of digits
        digest = md5_hash.digest()

        encoded_id = base64.b64encode(digest)
        short_id = encoded_id.decode()
        return short_id

    def update_value_in_df(
        self, df: pl.DataFrame, column_name, row_idx, pl_dtype, new_value
    ) -> pl.DataFrame:
        """Update value in a df. If the column doesn't exist, automatically generate one."""
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
    t.append_table_from_matrix(df)
