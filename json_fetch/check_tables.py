"""Generate tables for each type of check plus a main table."""
import polars as pl
import json
import hashlib
import base64
from typing import List, Dict
from collections import defaultdict
import pprint

CHECK_KEY = "check"
COMMAND_KEY = "command"
CHECKS_LIST_KEY = "checks"
UID_VAR = "uid"
class MainTable:
    def __init__(self) -> None:
        pass


class CheckTable:
    def __init__(self) -> None:
        pass


class CommandTable:
    def __init__(self) -> None:
        pass


class TableManager:
    def __init__(self, table_path) -> None:
        self.table_path = table_path

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
            row_variables = observations_without_insight.drop([""]).row(row_idx) if "" in observations_without_insight.columns else observations_without_insight.row(row_idx)
            # Generate an unique string based on the information of one row
            # To generate an uid
            row_unique_string = " ".join(str(item) for item in row_variables)
            uid = self.generate_uid(row_unique_string)

            # Store the uid in the main table
            observations_without_insight = self.update_value_in_df(observations_without_insight, UID_VAR, row_idx, pl.Utf8,uid)


            insight_dict = json.loads(insights[row_idx])
            insight_metadata, insight_checks = self.triage_checks(insight_dict)

            # embed the insight metadata to the main dataframe
            for insight_info in insight_metadata:
                inf_value = insight_metadata[insight_info]
                observations_without_insight = self.update_value_in_df(observations_without_insight, insight_info, row_idx, pl.Int64,inf_value)
            
        print(observations_without_insight)

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

    def update_value_in_df(self, df:pl.DataFrame, column_name, row_idx, pl_dtype, new_value)-> pl.DataFrame:
        """Update value in a df. If the column doesn't exist, automatically generate one."""
        # If no such an column exists in df, then generate a null column with this column
        if column_name not in df.columns:
            df = df.with_columns(pl.lit(None).alias(column_name).cast(pl_dtype))
        
        # Update the value of column in a specific row
        df[row_idx, column_name] = new_value
        return df
        
        
if __name__ == "__main__":
    df = pl.read_csv("report.csv")
    t = TableManager(".")
    t.append_table_from_matrix(df)
