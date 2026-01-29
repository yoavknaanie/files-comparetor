"""
Created in 2025
@author: Yoav Knaanie
"""
from datetime import datetime
import re
import chardet
import excel_to_csv as etc
import os
from pathlib import Path
import pandas as pd
import csv
import time
import sys
import gc
# import pyarrow
from openpyxl import Workbook
import os
from typing import Iterable, Tuple
import math


class Comparetor:
    """
    Compares two files

    Parameters
    ----------
    file1_filename : str
        Path to the first file.
    file1_separator : str
        Separator (delimiter) used in the first file (e.g., "," or "\t").
    mapping_1_to_2 : str
        Column mapping between file1 and file2, in the format:
        "(col_num1_a:col_num2_x),(col_num1:col_num2),..."
        Example: "(1:2),(3:4)" will map column 1 in file1 to column 2 in file2,
        and column 3 in file1 to column 4 in file2.
    file1_has_header : str
        Whether the first file has a header row. Must be "True" or "False".
    file1_date_format : str
        Date format/regex pattern to parse dates in file1.
        Should use named groups (?P<year>, ?P<month>, ?P<day>).
    file2_filename : str
        Path to the second file.
    file2_separator : str
        Separator (delimiter) used in the second file.
    file2_has_header : str
        Whether the second file has a header row. Must be "True" or "False".
    file2_date_format : str
        Date format/regex pattern to parse dates in file2.
    SAP_encoding=None : file_with_2_columns:column_in_file1_to_encode
    allow_fixing_csv=True : internal parameter for creating formated "fixed_" file
    splitting_col1="0": the column according to which we splitting file 1
    splitting_col2="0": the column according to which we splitting file 2

    Attributes
    ----------
    mapping_1_to_2 : dict
        Dictionary mapping file1 columns to file2 columns.
    cols_mapping_2_to_1 : dict
        Reverse mapping from file2 columns to file1 columns.
    file1_has_header : bool
        True if file1 has a header, False otherwise.
    file2_has_header : bool
        True if file2 has a header, False otherwise.
    file1_date_format : str
        Regex pattern for parsing file1 dates.
    file2_date_format : str
        Regex pattern for parsing file2 dates.
    """

    OUTPUT_FILE_PATH = "result.txt"
    # DATE_TO_REGEX_DICT = {"year":r"\b\d{4}\b", "day":r"\b\d{2}\b", "month":r"\b(0[1-9]|1[0-2])\b"}
    DATE_TO_REGEX_DICT = {
        "year": r"(?P<year>\d{4})",
        "month": r"(?P<month>0[1-9]|1[0-2])",
        "day": r"(?P<day>0[1-9]|[12]\d|3[01])",
    }
    GARBAGE_VALUES = ["SUB|SUB|SUB"]
    # PARSERS = lambda val: "" if val == "SUBSUBSUB" else val
    EXCLE_SUFFIX = {".xlsx", ".xls", "xlsm", ".xlsb"}
    MISSING_VAL_HEADER = "Header of the missing value"
    ENCODING_OUTPUT_FORMAT = "utf-8-sig"
    ENCODING_FIXED_INPUT_FORMAT = "utf-8-sig"
    ENCODING_FIXED_TXT_INPUT_FORMAT = "cp1255"
    PARTIAL_FILES_ROWS_NUM = 10000
    SPLITS_DIR = "splits"

    def __init__(
        self,
        file1_filename,
        file1_separator,
        file1_key_columns,
        mapping_1_to_2,
        file1_has_header: str,
        file1_date_format,
        file1_date_columns,
        # file 2 parameters
        file2_filename,
        file2_separator,
        file2_key_columns,
        file2_has_header,
        file2_date_format,
        file2_date_columns,
        SAP_encoding=None,
        allow_fixing_csv=True,
        splitting_col1="0",
        splitting_col2="0",
    ):
        # file1 reading
        self.file1_splitting_col, self.file2_splitting_col = int(splitting_col1), int(splitting_col2)
        self.file1_data_dict = None
        self.file2_data_dict = None
        self.file1_headers = None
        self.file2_headers = None
        self.file1_data_df = None
        self.file2_data_df = None
        self.res_unique1_path = None
        self.res_unique2_path = None
        self.res_diffs_path = None

        self.file1_filename = self.check_and_fix_csv_file(file1_filename, allow_fixing_csv)
        # self.file1_filename = file1_filename
        self.file1_separator = file1_separator
        self.mapping_1_to_2 = self.init_mappin_1_to_2(mapping_1_to_2)
        self.file1_has_header = self.init_file_has_header(file1_has_header)
        self.file1_date_format = self.init_file_date_format(file1_date_format)
        self.file1_date_columns = file1_date_columns.split(",")
        for i, x in enumerate(self.file1_date_columns):
            self.file1_date_columns[i] = int(x)
        self.file1_key_columns = file1_key_columns.split(",")
        for i, x in enumerate(self.file1_key_columns):
            self.file1_key_columns[i] = int(x)

        # file2 reading
        self.file2_filename = self.check_and_fix_csv_file(file2_filename, allow_fixing_csv)
        # self.file2_filename = self.check_and_fix_csv_file(file2_filename)
        self.file2_separator = file2_separator
        # mapping back
        self.cols_mapping_2_to_1 = {}
        for key in self.mapping_1_to_2:
            self.cols_mapping_2_to_1[self.mapping_1_to_2[key]] = key
        self.file2_has_header = self.init_file_has_header(file2_has_header)
        self.file2_date_format = self.init_file_date_format(file2_date_format)
        self.file2_date_columns = file2_date_columns.split(",")
        for i, x in enumerate(self.file2_date_columns):
            self.file2_date_columns[i] = int(x)
        self.file2_key_columns = file2_key_columns.split(",")
        for i, x in enumerate(self.file2_key_columns):
            self.file2_key_columns[i] = int(x)

        self.df1 = None
        self.df2 = None
        self.dif_df = None
        self.SAP_encoding = None
        if SAP_encoding:
            # init_decode_SAP is a list of dicts col_num:mapping dict
            self.SAP_encoding = self.init_decode_SAP(SAP_encoding)

    # from file 1 to file 2
    def check_and_fix_csv_file(self, path, allow_fixing_csv):
        if path.endswith(".csv") and allow_fixing_csv:
            print("fixing csv file while saving the old copy")
            return etc.convert_to_proper_csv(path)
        else:
            return path

    def clean_unprintable_chars(self, filthy_string):
        return "".join(ch for ch in filthy_string if ch.isprintable())

    def init_file_date_format(self, file1_date_format):
        formated = rf"{file1_date_format}"
        formated = formated.replace("year", self.DATE_TO_REGEX_DICT["year"])
        formated = formated.replace("day", self.DATE_TO_REGEX_DICT["day"])
        formated = formated.replace("month", self.DATE_TO_REGEX_DICT["month"])
        return formated

    def init_mappin_1_to_2(self, mapping_1_to_2: str) -> dict[int, int]:
        """
        Parse a mapping string like "(1:2),(3:4)" into {1|2, 3|4}.
        """
        # allow spaces: "( 3 : 3 )" though not recommended
        r"\((\d+):(\d+)\)"
        pairs = re.findall(r"\((\d+)\|(\d+)\)", mapping_1_to_2)
        if not pairs:
            raise ValueError(f"Invalid mapping string: {mapping_1_to_2!r}")
        map_dict = dict(pairs)
        res_dict = {}
        for key, val in map_dict.items():
            res_dict[int(key)] = int(val)
        return res_dict

    def init_file_has_header(self, has_header):
        if has_header == "True":
            return True
        elif has_header == "False":
            return False
        else:
            raise ValueError("file1_has_header should be in format True/False!")

    def format_date(self, pattern: str, date_as_string: str) -> datetime:
        """
        Parse a date string using a regex pattern with named groups.

        Parameters:
        pattern (str): Regex with named groups (?P<year>, ?P<month>, ?P<day>)
        date_as_string (str): The input date string

        Returns:
        datetime: Parsed datetime object
        """
        # todo can also apply fullmatch
        match = re.match(pattern, date_as_string)
        if not match:
            # print(f"Date '{date_as_string}' does not match pattern '{pattern}'")
            return datetime(1, 1, 1)  # 1/1/1 for missing or invalid dates
        year = int(match.group("year"))
        month = int(match.group("month"))
        day = int(match.group("day"))
        return datetime(year, month, day)

    def clean_line_from_invisibles(self, words: list):
        for i, x in enumerate(words):
            words[i] = self.clean_unprintable_chars(x).strip()
        return words

    def decode_SAP_values(self, words):
        if not self.SAP_encoding:
            return words
        for SAP_encode in self.SAP_encoding:
            for col in SAP_encode.keys():
                if words[col] in SAP_encode[col].keys():
                    new_val = SAP_encode[col][words[col]]
                    words[col] = new_val
        return words

    def load_file1_csv_into_dict(self):
        """
        :return: dict where:
        keys columns concutinated:
        """
        file_all_data = {}
        headers = []
        # self.check_excel(self.file1_filename)
        with open(
            self.file1_filename,
            "r",
            encoding=self.ENCODING_FIXED_INPUT_FORMAT,
        ) as file:
            reader = csv.reader(file, delimiter=self.file1_separator)
            line_index = 0
            number_of_invalid_lines = 0
            for words in reader:
                line_index += 1
                # preprocssing values, date, garbitch
                # format dates
                words = self.clean_line_from_invisibles(words)
                if self.file1_has_header:
                    self.file1_headers = self.init_headers(words, self.file1_key_columns)
                    self.file1_has_header = False
                    continue
                elif line_index == 1:
                    self.file1_headers = [f"col {i}" for i in enumerate(words + ["_"])]
                words = self.clean_words(words)
                try:
                    for i in self.file1_date_columns:
                        words[i] = self.format_date(
                            self.file1_date_format, words[i].strip()
                        ).strftime("%Y-%m-%d")
                    words = self.decode_SAP_values(words)
                    # insert keys cols
                    unique_key = ""
                    for i in self.file1_key_columns:
                        unique_key += " " + words[i]
                    unique_key = unique_key[1:]
                    file_all_data[unique_key] = words
                except (ValueError, IndexError) as e:
                    # row-specific, recoverable issues: log and skip the line
                    # print(f"[load_file1_into_dict] Line {line_index}: {e}. Skipping.")
                    number_of_invalid_lines += 1
                    print("value error in line ", number_of_invalid_lines)
                    continue
                except Exception as e:
                    # unexpected error: log and re-raise (or `continue` if you prefer to skip)
                    # print(f"[load_file1_into_dict] Line {line_index}: unexpected error: {e!r}")
                    raise
        self.file1_data_dict = file_all_data
        self.df1 = pd.DataFrame(columns=self.file1_headers, dtype=str)
        print(f"there are {line_index} rows in {self.file1_filename}")
        print(f"there are {number_of_invalid_lines} invalid rows in {self.file1_filename}")

    def load_file2_csv_into_dict(self):
        """
        :return: dict where:
        keys columns concutinated:
        """
        file_all_data = {}
        headers = []
        # self.check_excel(self.file1_filename)
        with open(
            self.file2_filename,
            "r",
            encoding=self.ENCODING_FIXED_INPUT_FORMAT,
        ) as file:
            reader = csv.reader(file, delimiter=self.file2_separator)
            line_index = 0
            number_of_invalid_lines = 0
            for words in reader:
                line_index += 1
                # preprocssing values, date, garbitch
                # format dates
                words = self.clean_words(words)
                words = self.clean_line_from_invisibles(words)
                if self.file2_has_header:
                    self.file2_headers = self.init_headers(words, self.file2_key_columns)
                    self.file2_has_header = False
                    continue
                elif line_index == 1:
                    self.file2_headers = [f"col {i}" for i in enumerate(words + ["_"])]
                try:
                    for i in self.file2_date_columns:
                        words[i] = self.format_date(
                            self.file2_date_format, words[i].strip()
                        ).strftime("%Y-%m-%d")
                    # insert keys cols
                    unique_key = ""
                    for i in self.file2_key_columns:
                        unique_key += " " + words[i]
                    unique_key = unique_key[1:]
                    # todo remove garbitch values
                    # to output a di
                    # todo which comparisons do I want to do? what cols to what cols?
                    # record = {}
                    # for i, x in enumerate(words):
                    # record[i] = words[i].strip()
                    # todo option: to give each colomn its type at the unput parameters file
                    # maybe more word treatment
                    file_all_data[unique_key] = words
                except (ValueError, IndexError) as e:
                    # row-specific, recoverable issues: log and skip the line
                    # print(f"[load_file1_into_dict] Line {line_index}: {e}. Skipping.")
                    number_of_invalid_lines += 1
                    print("value error in line ", number_of_invalid_lines)
                    continue
                except Exception as e:
                    # unexpected error: log and re-raise (or `continue` if you prefer to skip)
                    # print(f"[load_file1_into_dict] Line {line_index}: unexpected error: {e!r}")
                    raise
        self.file2_data_dict = file_all_data
        self.df2 = pd.DataFrame(columns=self.file2_headers, dtype=str)
        print(f"there are {line_index} rows in {self.file2_filename}")
        print(f"there are {number_of_invalid_lines} invalid rows in {self.file2_filename}")

    def init_headers(self, words, key_columns):
        unique_key = ""
        for i in key_columns:
            unique_key += " " + words[i]
        unique_key = unique_key[1:]
        return words + [unique_key]

    def load_file1_into_dict(self):
        """
        :return: dict where:
        keys columns concutinated:
        """
        file_all_data = {}
        headers = []
        # self.check_excel(self.file1_filename)
        with open(self.file1_filename, "r", encoding=self.ENCODING_FIXED_INPUT_FORMAT) as file:
            line_index = 0
            number_of_invalid_lines = 0
            for line in file:
                line_index += 1
                # preprocssing values, date, garbitch
                # format dates
                line = self.clean_line(line)
                words = line.split(self.file1_separator)
                words = self.clean_line_from_invisibles(words)
                if self.file1_has_header:
                    self.file1_headers = self.init_headers(words, self.file1_key_columns)
                    self.file1_has_header = False
                    self.file1_headers
                    continue
                elif line_index == 1:
                    self.file1_headers = [f"col {i}" for i in enumerate(words + ["_"])]
                try:
                    for i in self.file1_date_columns:
                        words[i] = self.format_date(
                            self.file1_date_format, words[i].strip()
                        ).strftime("%Y-%m-%d")
                    words = self.decode_SAP_values(words)
                    # insert keys cols
                    unique_key = ""
                    for i in self.file1_key_columns:
                        unique_key += " " + words[i]
                    unique_key = unique_key[1:]
                    file_all_data[unique_key] = words
                except (ValueError, IndexError) as e:
                    # row-specific, recoverable issues: log and skip the line
                    # print(f"[load_file1_into_dict] Line {line_index}: {e}. Skipping.")
                    number_of_invalid_lines += 1
                    print("value error in line " + number_of_invalid_lines)
                    continue
                except Exception as e:
                    # unexpected error: log and re-raise (or `continue` if you prefer to skip)
                    # print(f"[load_file1_into_dict] Line {line_index}: unexpected error: {e!r}")
                    raise
        self.file1_data_dict = file_all_data
        self.df1 = pd.DataFrame(columns=self.file1_headers, dtype=str)
        print(f"there are {line_index} rows in {self.file1_filename}")
        print(f"there are {number_of_invalid_lines} invalid rows in {self.file1_filename}")

    def load_file2_into_dict(self):
        """
        :return: dict where:
        keys columns concutinated:
        """
        file_all_data = {}
        with open(self.file2_filename, "r", encoding=self.ENCODING_FIXED_INPUT_FORMAT) as file:
            # with open(self.file2_filename, "r", encoding="windows-1251") as file:
            number_of_invalid_lines = 0
            line_index = 0
            for line in file:
                line_index += 1
                # preprocssing values, date, garbitch
                # format dates
                line = self.clean_line(line)
                words = line.split(self.file2_separator)
                words = self.clean_line_from_invisibles(words)
                if self.file2_has_header:
                    self.file2_headers = self.init_headers(words, self.file2_key_columns)
                    self.file2_has_header = False
                    continue
                elif line_index == 1:
                    self.file2_headers = [f"col {i}" for i in enumerate(words + ["_"])]
                try:
                    # format dates
                    for i in self.file2_date_columns:
                        words[i] = self.format_date(
                            self.file2_date_format, words[i]
                        ).strftime("%Y-%m-%d")
                    # insert keys cols
                    unique_key = ""
                    for i in self.file2_key_columns:
                        unique_key += " " + words[i]
                    unique_key = unique_key[1:]
                    file_all_data[unique_key] = words
                except (ValueError, IndexError) as e:
                    # row-specific, recoverable issues: log and skip the line
                    # print(f"[load_file2_into_dict] Line {line_index}: {e}. Skipping.")
                    number_of_invalid_lines += 1
                    print("value error in line " + number_of_invalid_lines)
                    continue
                except Exception as e:
                    # unexpected error: log and re-raise (or `continue` if you prefer to skip)
                    # print(f"[load_file2_into_dict] Line {line_index}: unexpected error: {e!r}")
                    raise
        self.file2_data_dict = file_all_data
        self.df2 = pd.DataFrame(columns=self.file2_headers, dtype=str)
        print(f"there are {line_index} rows in {self.file2_filename}")
        print(f"there are {number_of_invalid_lines} invalid rows in {self.file2_filename}")

    def clean_line(self, line):
        line = line.strip()
        for garval in self.GARBAGE_VALUES:
            line = line.replace(garval, "")
        return line

    def clean_words(self, words):
        for word in words:
            if word in self.GARBAGE_VALUES:
                word = ""
        return words

    def init_dif_df(self):
        if self.file1_headers:
            key_headers = [self.file1_headers[i] for i in self.file1_key_columns]
        elif self.file2_headers:
            key_headers = [self.file2_headers[i] for i in self.file2_key_columns]
        else:
            key_headers = [f"{self.file1_filename}_i" for i in self.file1_key_columns]
        headers = key_headers + [
            f"value in {Path(self.file1_filename).stem}",
            f"value in {Path(self.file2_filename).stem}",
        ]
        return pd.DataFrame(columns=headers, dtype=str)

    def load_files(self):
        # loading dictioneries
        print("starts loading files")
        if self.file1_filename.endswith(".csv"):
            self.load_file1_csv_into_dict()
        else:
            self.load_file1_into_dict()
        if self.file2_filename.endswith(".csv"):
            self.load_file2_csv_into_dict()
        else:
            self.load_file2_into_dict()
        # pd.DataFrame(columns=self.file1_headers)
        # print("starts creating pandas df files")

        # creates data df in adition
        # self.file1_data_df = pd.DataFrame(columns=self.file1_headers, dtype=str)
        # self.file2_data_df = pd.DataFrame(columns=self.file2_headers, dtype=str)
        # for key, raw in self.file1_data_dict.items():
        # raw = raw + [key]
        # self.file1_data_df.loc[len(self.file1_data_df)] = raw
        # for key, raw in self.file2_data_dict.items():
        # raw = raw + [key]
        # self.file2_data_df.loc[len(self.file2_data_df)] = raw
        # clean original dicts
        # self.file1_data_dict = None
        # self.file2_data_dict = None
        gc.collect()
        print("finished load_files")

    def init_decode_SAP(sel, SAP_encoding):
        encodes = SAP_encoding.split(",")
        mid_dic = {int(parse_me.split(":")[1]): parse_me.split(":")[0] for parse_me in encodes}
        encoding_dictionaries = []
        for col, path in mid_dic.items():
            encoding_dict = {}
            df = None
            try:
                df = pd.read_excel(path, engine="xlrd", dtype=str)
                print(
                    f"successed open {path} with:pd.read_excel\
(path, sheet_name=None, engine=xlrd"
                )
            except Exception:
                print("pd.read_csv(path) failed for SAP_decoding_path: " + path)
            try:
                df = pd.read_excel(path, engine="openpyxl", dtype=str)
                print(
                    f"successed open {path} with:pd.read_excel(path, \
sheet_name=None, engine=openpyxl"
                )
            except Exception:
                print(
                    "fail reading csv as openpyxl failed for SAP_decoding_pat: \
"
                    + path
                )
            try:
                df = pd.read_csv(path, encoding="utf-8", dtype=str)
                is_excel = False
                print(
                    f"successed open {path} with:\
pd.read_csv(csv_path , encoding=utf-8 failed for SAP_decoding_pat"
                )
            except Exception:
                print(
                    "fail reading csv SAP_decoding_pat pd.read_csv(\
path, sheet_name=None, encoding=utf-8: "
                    + path
                )
            try:
                df = pd.read_csv(path, encoding="cp1255", dtype=str)
                is_excel = False
                print(f"successed open {path} with: pd.read_csv(path, encoding=cp1255")
                encoding_format = "cp1255"
            except Exception:
                print(
                    "fail reading csv SAP_decoding_pat \
pd.read_csv(path, sheet_name=None, encoding=cp125: "
                    + path
                )
                raise
            col_dict = {}
            for i in range(len(df)):
                original = df.iloc[i, 0]
                decoded = df.iloc[i, 1]
                col_dict[original] = decoded
            encoding_dict[col] = col_dict
            encoding_dictionaries.append(encoding_dict)
        return encoding_dictionaries

    def dict_comapre(self):
        pass

    # usecols=list(self.mapping_1_to_2.keys() | self.mapping_1_to_2.values() | set(self.file1_key_columns))
    def differences_pandas(self):
        # differences
        # using pandas-------------------
        keys1 = self.file1_data_df.iloc[:, -1]
        keys2 = self.file2_data_df.iloc[:, -1]
        merged = self.file1_data_df.merge(
            self.file2_data_df,
            left_on=keys1,
            right_on=keys2,
            how="inner",
        ).iloc[:, 1:]
        left_cols = [i + 1 for i in self.mapping_1_to_2.keys()]
        right_cols = [i + len(self.file1_headers) + 1 for i in self.mapping_1_to_2.keys()]
        cols_to_check = [0] + left_cols + right_cols
        dif_df = self.init_dif_df()
        for left, right in self.mapping_1_to_2.items():
            right += len(self.file1_headers)
            filtered_merged = merged[merged.iloc[:, left] != merged.iloc[:, right]]
            cols_for_output = self.file1_key_columns + [left, right]
            differences_mini_df = filtered_merged.iloc[:, cols_for_output]
            differences_mini_df.columns = dif_df.columns
            dif_df = pd.concat([dif_df, differences_mini_df], ignore_index=True)
        self.dif_df = dif_df
        self.res_diffs_path = (
            fr"differences_{Path(self.file1_filename).stem}_{Path(self.file2_filename).stem}.csv"
        )
        self.dif_df.to_csv(
            self.res_diffs_path,
            index=False,
            encoding=self.ENCODING_OUTPUT_FORMAT,
        )

    def differences_duck(self):
        pass

    # key11 = self.file1_data_df.iloc[-1]
    # key22 = self.file2_data_df.iloc[-1]
    # cols to the dif output
    # carry_cols = list(self.file1_key_columns) if hasattr(self, "file1_key_columns") else []
    # if key11 not in carry_cols:
    # carry_cols.append(key11)
    # carry_cols = list(self.mapping_1_to_2.keys() | self.mapping_1_to_2.values())
    # carry_cols = [self.file1_headers[i] for i in carry_cols]
    # 4
    # selects = []
    # for left_col, right_col in self.mapping_1_to_2.items():
    #     # null safe inequality
    #     selected_cols = ", ".join([f'a."{c}" AS "{c}"' for c in carry_cols])
    #     sel = f"""
    #     SELECT
    #     {selected_cols},
    #     '{left_col}' AS compared_column,
    #     a."{left_col}" AS left_value,
    #     b."{right_col}" AS right_value,
    #     FROM t1 a
    #     JOIN t2 b
    #     ON a."{key11}" = b."{key22}"
    #     WHERE a."{left_col}" IS DISTINCT FROM b."{right_col}"
    #     """
    #     selects.append(sel)
    # union_sql = "\nUNION ALL\n".join(selects)
    # print(f"the sql command was {union_sql}")
    # # if persist:
    # #     con.execute(f"CREATE OR REPLACE TABLE differences AS {union_sql}")
    # result_df = con.execute(union_sql).df()
    # #free
    # con.unregister("t1")
    # con.unregister("t2")
    # result_df.to_csv(
    #     fr"differences_{Path(self.file1_filename).stem}_{Path(self.file2_filename).stem}.csv",
    #     index=False, encoding=self.ENCODING_OUTPUT_FORMAT)

    def differences_dicts(self):
        # diffrences with dicts ----------------
        # result.write("\n")
        # differences
        dif_df = self.init_dif_df()
        for key1 in set(self.file1_data_dict) & set(self.file2_data_dict):
            record1 = self.file1_data_dict[key1]
            record2 = self.file2_data_dict[key1]
            for col in self.mapping_1_to_2:  # todo optional to perform the comparisons for each two cols seperatly every place
                if record1[col] != record2[self.mapping_1_to_2[col]]:
                    # todo possible to write here the colomns name from an extra dict for the headers
                    # todo optional adding the columns headers of the missing value (in 2 new cols)
                    added_row = [record1[i] for i in self.file1_key_columns]  # key vals
                    added_row = added_row + [record1[col], record2[col]]  # diffrent vals
                    dif_df.loc[len(dif_df)] = added_row
        # end diffrences with dicts ----------------
        self.dif_df = dif_df
        self.dif_df.to_csv(
            fr"differences_{Path(self.file1_filename).stem}_{Path(self.file2_filename).stem}.csv",
            index=False,
            encoding=self.ENCODING_OUTPUT_FORMAT,
        )

    def duck_compare(self, unique1=True, unique2=True, differences=True):
        pass

    # print("starts duck compare compare")
    # strarting_time = time.time()
    # key_header1 = self.file1_headers[-1]
    # key_header2 = self.file2_headers[-1]
    # con = duckdb.connect(f"{self.file1_filename}_{self.file1_filename}.duckdb")
    # con.register("t1_tmp", self.file1_data_df)
    # con.register("t2_tmp", self.file2_data_df)
    # con.execute("CREATE OR REPLACE TABLE t1 AS SELECT * FROM t1_tmp")
    # con.execute("CREATE OR REPLACE TABLE t2 AS SELECT * FROM t2_tmp")
    # con.unregister("t1_tmp")
    # con.unregister("t2_tmp")
    # del self.file1_data_df
    # del self.file2_data_df
    # gc.collect()
    # first_10_cols = con.execute("SELECT * FROM t1 LIMIT 10").df()
    # print(first_10_cols)
    # # merge_query = f"SELECT * FROM t1 a JOIN t2 b ON 'a.{key_header1}' = 'b.{key_header2}'"
    # # merge_query = f"SELECT * FROM t1 a JOIN t2 b"
    # # merged = con.execute(merge_query).df()
    # # print(merged)
    # print()
    # ending_time = time.time()
    # print("loading files took:", ending_time-strarting_time)
    # if unique1:
    #     strarting_time = time.time()
    #     mask = ~self.file1_data_df.iloc[:, -1].isin(
    #         self.file2_data_df.iloc[:, -1])
    #     self.df1 = self.file1_data_df[mask]
    #     ending_time = time.time()
    #     self.df1.to_csv(fr"unique_records_in_{Path(self.file1_filename).stem}.csv",
    #                     index=False, encoding=self.ENCODING_OUTPUT_FORMAT)
    #     print("Runtime of unique1 was:", ending_time-strarting_time)
    #     self.df1 = None #clean RAM
    # if unique2:
    #     strarting_time = time.time()
    #     mask = ~self.file2_data_df.iloc[:, -1].isin(
    #         self.file1_data_df.iloc[:, -1])
    #     self.df2 = self.file2_data_df[mask]
    #     self.df2.to_csv(fr"unique_records_in_{Path(self.file2_filename).stem}.csv",
    #                     index=False , encoding=self.ENCODING_OUTPUT_FORMAT)
    #     ending_time = time.time()
    #     print("Runtime of unique2 was:", ending_time-strarting_time)
    #     self.df2 = None #clean RAM
    # if differences:
    #     strarting_time = time.time()
    #     self.differences_duck()
    #     ending_time = time.time()
    #     # ----- end with pandas
    #     print("Runtime of differences was:", ending_time-strarting_time)

    def pandas_compare(self, unique1=True, unique2=True, differences=True):
        self.file1_data_dict = None
        self.file2_data_dict = None
        print("starts pandas compare")
        self.res_unique2_path = None

        if unique1:
            strarting_time = time.time()
            mask = ~self.file1_data_df.iloc[:, -1].isin(
                self.file2_data_df.iloc[:, -1]
            )
            self.df1 = self.file1_data_df[mask]
            ending_time = time.time()
            self.res_unique1_path = (
                fr"unique_records_in_{Path(self.file1_filename).stem}.csv"
            )
            self.df1.to_csv(
                self.res_unique1_path,
                index=False,
                encoding=self.ENCODING_OUTPUT_FORMAT,
            )
            print("Runtime of unique1 was:", ending_time - strarting_time)
            self.df1 = None  # clean RAM

        if unique2:
            strarting_time = time.time()
            mask = ~self.file2_data_df.iloc[:, -1].isin(
                self.file1_data_df.iloc[:, -1]
            )
            self.df2 = self.file2_data_df[mask]
            self.res_unique2_path = (
                fr"unique_records_in_{Path(self.file2_filename).stem}.csv"
            )
            self.df2.to_csv(
                self.res_unique2_path,
                index=False,
                encoding=self.ENCODING_OUTPUT_FORMAT,
            )
            ending_time = time.time()
            print("Runtime of unique2 was:", ending_time - strarting_time)
            self.df2 = None  # clean RAM

        if differences:
            strarting_time = time.time()
            # differences
            # using pandas-------------------
            # self.dif_df = self.differences_pandas()
            self.differences_pandas()
            ending_time = time.time()
            # ----- end with pandas
            print("Runtime of differences was:", ending_time - strarting_time)

        # end of pandas code-------------------
        # not importent! del
        # merged = merged.iloc[:,cols_to_check]
        # merged = merged[merged.iloc[:, left_cols] == merged.iloc[:, right_cols]]
        # dif_df_headers = [self.file1_data_df.columns[i] for i in self.file1_key_columns]
        # dif_df_headers += [self.file1_filename + " value", self.file2_filename + " value"]
        # dif_df = pd.DataFrame(columns=dif_df_headers)
        # diffrences with dicts ----------------
        # # result.write("\n")
        # # differences
        # result.write("\n")
        # for key1 in set(self.file1_data_dict) & set(self.file2_data_dict):
        #     record1 = self.file1_data_dict[key1]
        #     record2 = self.file2_data_dict[key1]
        #     for col in self.mapping_1_to_2: #todo optional to perform the comparisons for each two cols seperatly every place
        #         if record1[col] != record2[self.mapping_1_to_2[col]]:
        #             result.write("difference for record id " + key1 + " col " + str(col) + " (" + str(
        #                 self.mapping_1_to_2[col]) + "):")
        #             # todo possible to write here the colomns name from an extra dict for the headers
        #             result.write(f"Value in {self.file1_filename} column {col}: {record1[col]} " +
        #                          f"Value in {self.file2_filename} column "+
        #                          f"{self.mapping_1_to_2[col]}: {record2[self.mapping_1_to_2[col]]}" + "\n")
        #             #todo optional adding the columns headers of the missing value (in 2 new cols)
        #             added_row = [record1[i] for i in self.file1_key_columns] # key vals
        #             added_row = added_row + [record1[col], record2[col]] # diffrent vals
        #             dif_df.loc[len(dif_df)] = added_row
        # result.write("end of report.")
        # end diffrences with dicts ----------------
        # return dif_df
        # # # # # # # #

    def regular_compare(self):
        # print = lambda s=None: result.write(("" if s is None else s) + "\n")
        # comparison logic

        # in file 1 but not in file 2
        self.df1.reset_index(drop=True)
        self.df2.reset_index(drop=True)
        strarting_time = time.time()
        for key1 in set(self.file1_data_dict) - set(self.file2_data_dict):
            self.df1.loc[len(self.df1)] = self.file1_data_dict[key1] + [
                key1
            ]  # adding a new row to result 1 df1
        self.res_unique1_path = (
            fr"unique_records_in_{Path(self.file1_filename).stem}.csv"
        )
        self.df1.to_csv(
            self.res_unique1_path,
            index=False,
            encoding=self.ENCODING_OUTPUT_FORMAT,
        )
        ending_time = time.time()
        print("Runtime of unique1 was:", ending_time - strarting_time)

        # in file 2 but not in file 1
        strarting_time = time.time()
        for key2 in set(self.file2_data_dict) - set(self.file1_data_dict):
            self.df2.loc[len(self.df2)] = self.file2_data_dict[key2] + [
                key2
            ]  # adding a new row to result 1 df1
        self.res_unique2_path = (
            fr"unique_records_in_{Path(self.file2_filename).stem}.csv"
        )
        self.df2.to_csv(
            self.res_unique2_path,
            index=False,
            encoding=self.ENCODING_OUTPUT_FORMAT,
        )
        ending_time = time.time()
        print("Runtime of unique2 was:", ending_time - strarting_time)

        # differences
        strarting_time = time.time()
        self.dif_df = self.init_dif_df()
        # for key1 in self.file1_data_dict:
        #     if key1 in self.file2_data_dict:
        for key1 in set(self.file1_data_dict) & set(self.file2_data_dict):
            record1 = self.file1_data_dict[key1]
            record2 = self.file2_data_dict[key1]
            for (
                col
            ) in self.mapping_1_to_2:  # todo optional to perform the comparisons for each two cols seperatly every place
                if record1[col] != record2[self.mapping_1_to_2[col]]:
                    # todo possible to write here the colomns name from an extra dict for the headers
                    # todo optional adding the columns headers of the missing value (in 2 new cols)
                    added_row = [record1[i] for i in self.file1_key_columns]  # key vals
                    added_row = added_row + [
                        record1[col],
                        record2[self.mapping_1_to_2[col]],
                    ]  # diffrent vals
                    self.dif_df.loc[len(self.dif_df)] = added_row
        self.res_diffs_path = (
            fr"differences_{Path(self.file1_filename).stem}_{Path(self.file2_filename).stem}.csv"
        )
        self.dif_df.to_csv(
            self.res_diffs_path,
            index=False,
            encoding=self.ENCODING_OUTPUT_FORMAT,
        )
        ending_time = time.time()
        print("Runtime of differences_ was:", ending_time - strarting_time)

    def just_compare(self):
        # self.pandas_compare(unique1=True, unique2=True, differences=True)
        self.regular_compare()

    def compare(self):
        self.load_files()
        result_path = (
            fr"{Path(self.file1_filename).stem}_{Path(self.file2_filename).stem}.xlsx"
        )
        self.just_compare()

    def create_tmp_full_files(self):
        # loading dictioneries
        print("starts create_tmp_full_files")
        if self.file1_filename.endswith(".csv"):
            self.create_tmp_full_file1_from_csv()
        else:
            self.create_tmp_full_file1()
        if self.file2_filename.endswith(".csv"):
            self.create_tmp_full_file2_from_csv()
        else:
            self.create_tmp_full_file2()
        # pd.DataFrame(columns=self.file1_headers)

    def create_tmp_full_file1(self):
        file_all_data = {}
        headers = []
        temp_full_file_path = Path(self.file1_filename).stem + "temp_full.csv"
        print(temp_full_file_path)
        with open(
            temp_full_file_path,
            "w",
            newline="",
            encoding=self.ENCODING_OUTPUT_FORMAT,
        ) as tmp_file:
            writer = csv.writer(tmp_file)
            # self.check_excel(self.file1_filename)
            with open(
                self.file1_filename,
                "r",
                encoding=self.ENCODING_FIXED_TXT_INPUT_FORMAT,
            ) as file:
                line_index = 0
                number_of_invalid_lines = 0
                for line in file:
                    line_index += 1
                    # preprocssing values, date, garbitch
                    # format dates
                    line = self.clean_line(line)
                    words = line.split(self.file1_separator)
                    words = self.clean_line_from_invisibles(words)
                    if self.file1_has_header:
                        self.file1_headers = self.init_headers(
                            words, self.file1_key_columns
                        )
                        self.file1_has_header = False
                        writer.writerow(
                            self.file1_headers[:-1]
                        )  # write the headers to the tmp file
                        continue
                    elif line_index == 1:
                        self.file1_headers = [
                            f"col {i}" for i in enumerate(words + ["_"])
                        ]
                        writer.writerow(
                            self.file1_headers[:-1]
                        )  # write the headers to the tmp file
                    try:
                        # for i in self.file1_date_columns:
                        #     words[i] = self.format_date(
                        #         self.file1_date_format, words[i].strip()).strftime("%Y-%m-%d")
                        writer.writerow(words)  # write the headers to the tmp file
                    except (ValueError, IndexError) as e:
                        # row-specific, recoverable issues: log and skip the line
                        # print(f"[load_file1_into_dict] Line {line_index}: {e}. Skipping.")
                        number_of_invalid_lines += 1
                        print("value error in line " + number_of_invalid_lines)
                        continue
                    except Exception as e:
                        # unexpected error: log and re-raise (or `continue` if you prefer to skip)
                        # print(f"[load_file1_into_dict] Line {line_index}: unexpected error: {e!r}")
                        raise
        self.file1_data_dict = file_all_data
        self.df1 = pd.DataFrame(columns=self.file1_headers, dtype=str)
        print(f"there are {line_index} rows in {self.file1_filename}")
        print(f"there are {number_of_invalid_lines} invalid rows in {self.file1_filename}")

    def create_tmp_full_file2(self):
        temp_full_file_path = Path(self.file2_filename).stem + "temp_full.csv"
        print(temp_full_file_path)
        with open(
            temp_full_file_path,
            "w",
            newline="",
            encoding=self.ENCODING_OUTPUT_FORMAT,
        ) as tmp_file:
            writer = csv.writer(tmp_file)
            # self.check_excel(self.file1_filename)
            with open(
                self.file2_filename,
                "r",
                encoding=self.ENCODING_FIXED_INPUT_FORMAT,
            ) as file:
                line_index = 0
                number_of_invalid_lines = 0
                for line in file:
                    line_index += 1
                    # preprocssing values, date, garbitch
                    # format dates
                    line = self.clean_line(line)
                    words = line.split(self.file2_separator)
                    words = self.clean_line_from_invisibles(words)
                    if self.file2_has_header:
                        self.file2_headers = self.init_headers(
                            words, self.file2_key_columns
                        )
                        self.file2_has_header = False
                        writer.writerow(
                            self.file2_headers[:-1]
                        )  # write the headers to the tmp file
                        continue
                    elif line_index == 1:
                        self.file1_headers = [
                            f"col {i}" for i in enumerate(words + ["_"])
                        ]
                        writer.writerow(
                            self.file2_headers[:-1]
                        )  # write the headers to the tmp file
                    try:
                        # for i in self.file2_date_columns:
                        #     words[i] = self.format_date(
                        #         self.file2_date_format, words[i].strip()).strftime("%Y-%m-%d")
                        writer.writerow(words)  # write the headers to the tmp file
                    except (ValueError, IndexError) as e:
                        # row-specific, recoverable issues: log and skip the line
                        # print(f"[load_file1_into_dict] Line {line_index}: {e}. Skipping.")
                        number_of_invalid_lines += 1
                        print("value error in line " + number_of_invalid_lines)
                        continue
                    except Exception as e:
                        # unexpected error: log and re-raise (or `continue` if you prefer to skip)
                        # print(f"[load_file1_into_dict] Line {line_index}: unexpected error: {e!r}")
                        raise

    def create_tmp_full_file1_from_csv(self):
        """
        for manual sorting
        1. creating the file in csv
        2. sort it handly
        3. split it into 10 csv files sorted by a given col
        4. aply load_files on each file (first, i should find a way to "clean" hte load files, or the comperator file)

        Returns
        -------
        None.
        """
        """
        :return: dict where:
        keys columns concutinated:
        """
        temp_full_file_path = Path(self.file1_filename).stem + "temp_full.csv"
        print(temp_full_file_path)
        with open(
            temp_full_file_path,
            "w",
            newline="",
            encoding=self.ENCODING_OUTPUT_FORMAT,
        ) as tmp_file:
            writer = csv.writer(tmp_file)
            with open(
                self.file1_filename,
                "r",
                encoding=self.ENCODING_FIXED_INPUT_FORMAT,
            ) as file:
                reader = csv.reader(file, delimiter=self.file1_separator)
                line_index = 0
                number_of_invalid_lines = 0
                for words in reader:
                    line_index += 1
                    # preprocssing values, date, garbitch
                    # format dates
                    words = self.clean_line_from_invisibles(words)
                    if self.file1_has_header:
                        self.file1_headers = self.init_headers(
                            words, self.file1_key_columns
                        )
                        self.file1_has_header = False
                        writer.writerow(
                            self.file1_headers[:-1]
                        )  # write the headers to the tmp file
                        continue
                    elif line_index == 1:
                        self.file1_headers = [
                            f"col {i}" for i in enumerate(words + ["_"])
                        ]
                        writer.writerow(
                            self.file1_headers[:-1]
                        )  # write the headers to the tmp file
                    words = self.clean_words(words)
                    try:
                        # for i in self.file1_date_columns:
                        #     words[i] = self.format_date(
                        #         self.file1_date_format, words[i].strip()).strftime("%Y-%m-%d")
                        # words = self.decode_SAP_values(words)
                        # creating temp full excel file
                        # writer csv work
                        writer.writerow(words)
                        
                    except (ValueError, IndexError) as e:
                        # row-specific, recoverable issues: log and skip the line
                        # print(f"[load_file1_into_dict] Line {line_index}: {e}. Skipping.")
                        number_of_invalid_lines += 1
                        # print("value error in line ", number_of_invalid_lines)
                        continue
                    
                    except Exception as e:
                        # unexpected error: log and re-raise (or `continue` if you prefer to skip)
                        # print(f"[load_file1_into_dict] Line {line_index}: unexpected error: {e!r}")
                        raise
        print("finished writing excel file")
        print(f"there are {line_index} rows in {self.file1_filename}")
        print(f"there are {number_of_invalid_lines} invalid rows in {self.file1_filename}")
        
    def create_tmp_full_file2_from_csv(self):
        """def create_tmp_full_file2_from_csv(self):
        
        for manual sorting
        1. creating the file in csv
        2. sort it handly
        3. split it into 10 csv files sorted by a given col
        4. aply load_files on each file (first, i should find a way to "clean" hte load files, or the comperator file)
        
        Returns
        -------
        None.
        """
        """
        :return: dict where:
        keys columns concutinated:
        """
        temp_full_file_path = Path(self.file2_filename).stem + "temp_full.csv"
        print(temp_full_file_path)
        with open(
        temp_full_file_path,
        "w",
        newline="",
        encoding=self.ENCODING_OUTPUT_FORMAT,
        ) as tmp_file:
            writer = csv.writer(tmp_file)
            with open(self.file2_filename, "r",
                  encoding=self.ENCODING_FIXED_INPUT_FORMAT,
     ) as file:
             reader = csv.reader(file, delimiter=self.file2_separator)
             line_index = 0
             number_of_invalid_lines = 0
             for words in reader:
                 line_index += 1
                 # preprocssing values, date, garbitch
                 # format dates
                 words = self.clean_line_from_invisibles(words)
                 if self.file2_has_header:
                     self.file2_headers = self.init_headers(
                         words, self.file2_key_columns
                     )
                     self.file2_has_header = False
                     writer.writerow(
                         self.file2_headers[:-1]
                     )  # write the headers to the tmp file
                     continue
                 elif line_index == 1:
                     self.file2_headers = [
                         f"col {i}" for i in enumerate(words + ["_"])
                     ]
                     writer.writerow(
                         self.file2_headers[:-1]
                     )  # write the headers to the tmp file
                 words = self.clean_words(words)
                 
                 try:
                     # for i in self.file2_date_columns:
                     #     words[i] = self.format_date(
                     #         self.file2_date_format, words[i].strip()).strftime("%Y-%m-%d")
                     # creating temp full excel file
                     # writer csv work
                     writer.writerow(words)
                 except (ValueError, IndexError) as e:
                     # row-specific, recoverable issues: log and skip the line
                     # print(f"[load_file1_into_dict] Line {line_index}: {e}. Skipping.")
                     number_of_invalid_lines += 1
                     # print("value error in line ", number_of_invalid_lines)
                     continue
                 except Exception as e:
                     # unexpected error: log and re-raise (or `continue` if you prefer to skip)
                     # print(f"[load_file1_into_dict] Line {line_index}: unexpected error: {e!r}")
                     raise
        print("finished writing excel file")
        print(f"there are {line_index} rows in {self.file2_filename}")
        print(f"there are {number_of_invalid_lines} invalid rows in {self.file2_filename}")
     
     # ---------------------------SPLITTING PART-------------------------------------
    def open_new_file(self, tmp_idx, header, encoding="utf-8-sig"):
        # open with a temporary name; we’ll rename to range on close
        tmp_path = os.path.join(self.SPLITS_DIR, f"part_tmp_{tmp_idx}.csv")
        f = open(tmp_path, "w", newline="", encoding=encoding)
        w = csv.writer(f)
        if header:
            w.writerow(header)
            return f, w, tmp_path
     
    def split_csv(self):
        """
        main task: generating files of 10000 rows length and return a list of their names
        algorithm:
        itirate through the rows of file1
        while file1 note ends:
        counter = 0
        if counter reaced 10000:
        - check value, continue itirating until it changes and keep
        writing to file f until the value changes
        - save this value and file name at splitter_information list.
        read file2:
        while file2 note ends:
        counter = 0
        if counter reaced 10000:
        - check value, continue itirating until it changes and keep
        writing to file f until the value changes
        - save this value and file name at splitter_information list.
        save the files
        return 2 lists
        (after that we will run comparetor on each file in the list with a diffrent comparetor object) --posiibly to reduse run time even more
        """
        print("Started split_csv1(file1_splitting_col)")
        partial1_list = self.split_csv1()
        print("finished split_csv1(file1_splitting_col)")
        partial2_list = self.split_csv2(partial1_list)
        print("finished split_csv2(file1_splitting_col)")
        # if "extras" in partial1_list[-1][0]:
        #     partial1_list.pop()
        partial1_list = [x[0] for x in partial1_list]
        print(partial1_list)
        print(partial2_list)
        
        return (partial1_list, partial2_list)
     
    def split_csv2(self,
         plan: list[Tuple[str, str]],# [(outfile_name, boundary_value), ...]
         output_dir: str = "splits",
         encoding: str = "utf-8-sig",
         ) -> None:
        """
        Split a sorted CSV (by column 0) into N files according to a plan of N tuples.
        For the *last* tuple, ignore its boundary and write until EOF.
        Each file includes the header. Streaming: no large RAM usage.
        
        plan[i] = (outfile_name, boundary_value) --> boundary is INCLUSIVE
        The last plan entry's boundary is ignored (acts as 'until EOF').
        """
        partial_information = []
        input_csv = f"{Path(self.file2_filename).stem}temp_full.csv"
        if not plan:
            print("ERROR! NO LIST PLAN FOR FILE 2 SPLITTING!")
            raise
        os.makedirs(output_dir, exist_ok=True)
        unique_extras_in_file2_path = "splits\\unique_extras.csv"
        updated_plan = plan + [(unique_extras_in_file2_path, math.inf)]
        print("plan is:")
        print(updated_plan)
        out_files = []
        with open(input_csv, "r", newline="", encoding=encoding) as fin:
            reader = csv.reader(fin)
            header = next(reader, None)
            pending_row = (
                None  # carry one row between files when we stop on a boundary
            )
            for idx, (outfile_name, boundary) in enumerate(updated_plan):
                if type(boundary) != float:
                    boundary = int(boundary.strip())
                last_file = idx == len(plan) - 1
                out_path = os.path.join(
                    output_dir, f"{Path(outfile_name).stem}_in_file2.csv"
                )
                with open(out_path, "w", newline="", encoding=encoding) as fout:
                    out_files.append(out_path)
                    w = csv.writer(fout)
                    if header:
                        w.writerow(header)
                    # seen_boundary = False
                    while True:
                        # get next row (from buffer or from reader)
                        if pending_row is not None:
                            row = pending_row
                            pending_row = None
                        else:
                            try:
                                row = next(reader)
                            except StopIteration:
                                row = None
                        if row is None:
                            # EOF: finish this file and we’re done for all files
                            # return out_files
                            break
                        if not row:  # skip empty lines safely
                            continue
                        # try:
                        group_val = int(row[self.file2_splitting_col].strip())
                        # except:
                        #     print(f"row[file2_splitting_col] is not in format of an int")
                        #     w.writerow(row)
                        if group_val <= boundary:
                            w.writerow(row)
                            continue
                        else:
                            pending_row = row
                            break
                        # Mark that we've reached the boundary (inclusive)
                        if group_val == boundary:
                            seen_boundary = True
        # print("jumped to end of function")
        return out_files
     
     # def split_csv2(
     #     self,
     #     plan: list[Tuple[str, str]], # [(outfile_name, boundary_value), ...]
     #     file2_splitting_col,
     #     output_dir: str = "splits",
     #     encoding: str = "utf-8-sig",
     # ) -> None:
     #     """
     #     Split a sorted CSV (by column 0) into N files according to a plan of N tuples.
     #     For the *last* tuple, ignore its boundary and write until EOF.
     #     Each file includes the header. Streaming: no large RAM usage.
     #
     #     plan[i] = (outfile_name, boundary_value) --> boundary is INCLUSIVE
     #     The last plan entry's boundary is ignored (acts as 'until EOF').
     #     """
     #     partial_information = []
     #     input_csv = f"{Path(self.file2_filename).stem}temp_full.csv"
     #     if not plan:
     #         print("ERROR! NO LIST PLAN FOR FILE 2 SPLITTING!")
     #         return
     #     unique_extras_in_file2_path = "splits\\unique_extras.csv"
     #     plan.append((unique_extras_in_file2_path, math.inf))
     #     print("plan is:")
     #     print(plan)
     #     os.makedirs(output_dir, exist_ok=True)
     #     out_files = []
     #     with open(input_csv, "r", newline="", encoding=encoding) as fin:
     #         reader = csv.reader(fin)
     #         header = next(reader, None)
     #         pending_row = None # carry one row between files when we stop on a boundary
     #         for idx, (outfile_name, boundary) in enumerate(plan):
     #             last_file = (idx == len(plan) - 1)
     #             out_path = os.path.join(output_dir, f"{Path(outfile_name).stem}_in_file2.csv")
     #             out_files.append(out_path)
     #             with open(out_path, "w", newline="", encoding=encoding) as fout:
     #                 w = csv.writer(fout)
     #                 if header:
     #                     w.writerow(header)
     #                 seen_boundary = False
     #                 while True:
     #                     # get next row (from buffer or from reader)
     #                     if pending_row is not None:
     #                         row = pending_row
     #                         pending_row = None
     #                     else:
     #                         try:
     #                             row = next(reader)
     #                         except StopIteration:
     #                             row = None
     #                     if row is None:
     #                         # EOF: finish this file and we’re done for all files
     #                         return out_files
     #                     if not row: # skip empty lines safely
     #                         continue
     #                     if last_file:
     #                         # Last file: ignore boundary, write everything to EOF
     #                         w.writerow(row)
     #                         continue
     #                     group_val = row[file2_splitting_col]
     #                     # If we already passed the boundary group and the value changed,
     #                     # stop this file; buffer current row for the next file.
     #                     if seen_boundary and group_val != boundary:
     #                         pending_row = row
     #                         break
     #                     # if int(group_val) > int(boundary):
     #                     #     pending_row = row
     #                     #     break
     #                     # Otherwise write row here
     #                     w.writerow(row)
     #                     # Mark that we've reached the boundary (inclusive)
     #                     if group_val == boundary:
     #                         seen_boundary = True
     #     return out_files
     
    def split_csv1(self, encoding="utf-8-sig"):
        partial_information = []
        input_file = f"{Path(self.file1_filename).stem}temp_full.csv"
        rows_per_file = self.PARTIAL_FILES_ROWS_NUM  # soft limit
        
        os.makedirs(self.SPLITS_DIR, exist_ok=True)
        
        with open(input_file, "r", newline="", encoding=encoding) as fin:
            reader = csv.reader(fin)
            header = next(reader, None)
            file_idx = 1
            fout, writer, tmp_path = self.open_new_file(file_idx, header)
            total_rows = 0  # data-row counter across the whole CSV (no header)
            rows_in_file = 0  # data rows in current output file
            file_start_row = 1  # first data-row number in the current file
            current_group_value = (
                None  # group = first column value in current file
            )
     
            for row in reader:
                if not row:
                    continue  # skip completely empty rows
                    
                sort_val = row[self.file1_splitting_col]
                # Initialize group on first data row
                if current_group_value is None:
                    current_group_value = sort_val
        
                # If we've passed the 10k soft limit AND the incoming row starts a new group,
                # close current file and start a new one.
                if rows_in_file >= rows_per_file and sort_val != current_group_value:
                    # finalize current file name with range
                    file_end_row = total_rows  # last written row number (global)
                    final_name = os.path.join(
                        self.SPLITS_DIR,
                        f"part_{file_start_row}_{file_end_row}.csv",
                    )
                    fout.close()
                    os.replace(tmp_path, final_name)
                    partial_information.append((final_name, current_group_value))
        
                    # start a new file
                    file_idx += 1
                    fout, writer, tmp_path = self.open_new_file(file_idx, header)
                    rows_in_file = 0
                    file_start_row = total_rows + 1
                    current_group_value = sort_val  # new group's value
         
                # write current row
                writer.writerow(row)
                rows_in_file += 1
                total_rows += 1
                
                # keep group value updated (in case data has anomalies)
                current_group_value = sort_val
         
             # finalize the last file (if any rows were written)
            if rows_in_file > 0:
                file_end_row = total_rows
                final_name = os.path.join(
                    self.SPLITS_DIR,
                    f"part_{file_start_row}_{file_end_row}.csv",
                )
                fout.close()
                os.replace(tmp_path, final_name)
                partial_information.append((final_name, current_group_value))
            else:
                # no data rows at all; close and remove the empty temp
                fout.close()
                try:
                    os.remove(tmp_path)
                except FileNotFoundError:
                    pass
        
        print(
            f"✅ Done. Wrote data rows 1..{total_rows} across files in '{self.SPLITS_DIR}'."
        )
        return partial_information
     