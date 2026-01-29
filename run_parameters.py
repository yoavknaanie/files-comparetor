"""
Created on Tue Sep 16 14:04:10 2025
@author: Yoav Knaanie
"""
###################################################################### text params
from comparetor import Comparetor
import time
import pickle
from pathlib import Path
import gc
import shutil
import os
import csv

PARAMETERS_PATH = "parameters.txt"
PARAMETERS_NUMBER = 17
SERIELIZE = False
SERIELIZE_OBJ_PATH = "fixed_92062_1000_fixed_matzav_nifga_STG_check_30000.pkl"
DESERIELIZE = False
RESULTS_BIG_FILE = "comparison_results"
SPLITTING_COL1, SPLITTING_COL2 = 0, 0  # cols of the mispar ishi for splitting the files
UNIQUE_EXTRAS_FILE2_FILE_NAME = "unique_extras.csv"

"""
README
1. first uncomment:
def big_files_comparison():
build_large_excel() <-------- uncomment
# part2() <------- comment
2. run with the parameters file format:
file1_filename file1_separator file1_key_columns mapping_1_to_2 file1_has_header
file1_date_format file1_date_columns file2_filename file2_separator
file2_key_columns file2_has_header file2_date_format file2_date_columns SAP_ecoding splitting_col1 splitting_col2 task_number(0=create big file\1=split and compare)
for example:
file1.txt | 0,1 (2|2)(3|3) False day.month.year 1 file2.csv | 0,1 False day.month.year 1 HAMARA_FILE.csv:3 file1_splittingcol file2_splitting_col
files may be .csv or .txt,
parameters example 2:
9206_2_source.csv , 0,1,4 (0|0) True yearmonthday 1 fixed_9206_2.csv , 0,2,6 True day/month/year 2 None 0 0 1
3. you will get the output for text file: file1temp_full.csv, and for csv file: fixed_file2temp_full.csv
open this files and sort them according to the splitting column (text of numerric values, mostly id number).
4. uncomment:
def big_files_comparison():
# build_large_excel() <-------- comment
part2() <------- uncomment
5. run the program with the paramters file:
file1.txt | 0,1 (2|2)(3|3) False day.month.year 1 fixed_file2.csv | 0,1 False day.month.year 1 HAMARA_FILE.csv:3 file1_splittingcol file2_splitting_col
files may be .csv or .txt,
* the names of the files should be the name of the files without temp_full
reuslts will be in the folder comparison_results
"""


def init_comparetor(convert_to_csv=True, given_file1_name=None, given_file2_name=None):
    with open(PARAMETERS_PATH, "r") as file:
        first_row = file.read()
        if len(first_row.split()) != PARAMETERS_NUMBER:
            raise ValueError(
                "Invalid number of parameters, format should be:\n"
                + "file1_filename, file1_separator, file1_key_columns, mapping_1_to_2, file1_has_header, file1_date_format, "
                "file1_date_columns, "
                "file2_filename, file2_separator, file2_key_columns, file2_has_header, file2_date_format,"
                "file2_date_columns, SAP_ecoding, splitting_col1, splitting_col2, task_number"
            )

        (
            file1_filename,
            file1_separator,
            file1_key_columns,
            mapping_1_to_2,
            file1_has_header,
            file1_date_format,
            file1_date_columns,
            file2_filename,
            file2_separator,
            file2_key_columns,
            file2_has_header,
            file2_date_format,
            file2_date_columns,
            SAP_ecoding,
            splitting_col1,
            splitting_col2,
            task_number,
        ) = first_row.split()

    if given_file1_name:
        file1_filename = given_file1_name
        file2_filename = given_file2_name

    if SAP_ecoding == "None":
        SAP_ecoding = None

    compare = Comparetor(
        file1_filename,
        file1_separator,
        file1_key_columns,
        mapping_1_to_2,
        file1_has_header,
        file1_date_format,
        file1_date_columns,
        file2_filename,
        file2_separator,
        file2_key_columns,
        file2_has_header,
        file2_date_format,
        file2_date_columns,
        SAP_ecoding,
        convert_to_csv,
        splitting_col1,
        splitting_col2,
    )
    return compare


# self, file1_filename, file1_separator, file1_key_columns,mapping_1_to_2, file1_has_header: str,
# file1_date_format, file1_date_columns,
# # file 2 parameters
# file2_filename, file2_separator, file2_key_columns, file2_has_header, file2_date_format, file2_date_columns):


def serielize(comperator):
    pickle_name = Path(comperator.file1_filename).stem + "_" + Path(comperator.file2_filename).stem + ".pkl"
    with open(pickle_name, "wb") as f:
        pickle.dump(comperator, f)
    print("serielized successfully")


def deserielize():
    with open(SERIELIZE_OBJ_PATH, "rb") as f:
        comparator = pickle.load(f)
    print("de-serielized successfully")
    return comparator


def original_task():
    starting_time = time.time()
    comparetor = None
    if not DESERIELIZE:
        comparetor = init_comparetor()
        comparetor.load_files()
    else:
        comparetor = deserielize()
    if SERIELIZE:
        serielize(comparetor)
    comparetor.just_compare()
    ending_time = time.time()
    print("Runtime was:", ending_time - starting_time)
    print("end successfully")


def build_large_excel():
    starting_time = time.time()
    comparetor = init_comparetor()
    comparetor.create_tmp_full_files()
    ending_time = time.time()
    print("Runtime was:", ending_time - starting_time)
    print("end successfully")


def split_excel():
    comparetor = init_comparetor(convert_to_csv=False)
    partial1_list, partial2_list = comparetor.split_csv()
    return partial1_list, partial2_list


def multiple_comparisons(partial1_list, partial2_list):
    result_folder = RESULTS_BIG_FILE
    os.makedirs(RESULTS_BIG_FILE, exist_ok=True)
    print("partial1_list and partial2_list")
    print(partial1_list)
    print(partial2_list)
    uniques_1 = []
    uniques_2 = []
    diffs = []

    for i, _ in enumerate(partial1_list):
        try:
            strarting_time = time.time()
            file1 = partial1_list[i]
            file2 = partial2_list[i]

            file1_in_comparetor = Path(file1).stem + ".csv"
            shutil.copy(file1, file1_in_comparetor)
            file2_in_comparetor = Path(file2).stem + ".csv"
            shutil.copy(file2, file2_in_comparetor)
            print(file1_in_comparetor)
            print(file2_in_comparetor)
            print("COMPARING THEM-----------------")

            comparetor = init_comparetor(
                given_file1_name=file1_in_comparetor,
                given_file2_name=file2_in_comparetor,
            )
            comparetor.load_files()
            comparetor.just_compare()
            print("-------------------------------------------------------------")
            print(f"finished comparing {file1_in_comparetor}, {file2_in_comparetor}")

            # deletes files in comparetor folder
            os.remove("fixed_" + file1_in_comparetor)
            os.remove("fixed_" + file2_in_comparetor)
            os.remove(file1_in_comparetor)
            os.remove(file2_in_comparetor)

            # maybe no need
            # move uniques partial file to destination folder
            src_unique1 = comparetor.res_unique1_path
            src_unique2 = comparetor.res_unique2_path
            src_diff = comparetor.res_diffs_path

            unique1 = f"{result_folder}\\{src_unique1}"
            unique2 = f"{result_folder}\\{src_unique2}"
            diff = f"{result_folder}\\{src_diff}"

            shutil.move(src_unique1, unique1)
            shutil.move(src_unique2, unique2)
            shutil.move(src_diff, diff)

            # differences_{Path(self.file1_filename).stem}_{Path(self.file2_filename).stem}.csv
            uniques_1.append(unique1)
            uniques_2.append(unique2)
            diffs.append(diff)

            ending_time = time.time()
            print("Runtime of the program is", ending_time - strarting_time)

        except Exception:
            print(f"failed on {file1} multiple_comparisons")
            raise

    # will be done
    unique_extras_file2_path = partial2_list[-1]
    unique_extras_file2_path_in_res = f"{result_folder}\\{UNIQUE_EXTRAS_FILE2_FILE_NAME}"
    shutil.move(unique_extras_file2_path, unique_extras_file2_path_in_res)
    uniques_2.append(unique_extras_file2_path_in_res)
    print("file:", unique_extras_file2_path_in_res, " created")

    # with open("uniques_and_dif_pathes.txt", "w") as f:
    # for x in uniques_1: dont want to write those in file because it is just the file names in the res folder
    return uniques_1, uniques_2, diffs


def union_results(uniques_1, uniques_2, small_diffs_list):
    comparetor = init_comparetor(convert_to_csv=False)
    unique_file1 = f"{RESULTS_BIG_FILE}\\unique_in_{Path(comparetor.file1_filename).stem}.csv"
    unique_file2 = f"{RESULTS_BIG_FILE}\\unique_in_{Path(comparetor.file2_filename).stem}.csv"
    diffs = f"{RESULTS_BIG_FILE}\\differences_{Path(comparetor.file1_filename).stem}_{Path(comparetor.file2_filename).stem}.csv"
    out_encoding = "utf-8-sig"

    print("starts combining-------------------")

    with open(unique_file1, "w", newline="", encoding=out_encoding) as f1:
        writer = csv.writer(f1)
        header_written = False
        for fname in uniques_1:
            with open(fname, "r", newline="", encoding=out_encoding) as fin:
                reader = csv.reader(fin)
                header = next(reader, None)
                if not header_written and header:  # write the header from the first file
                    writer.writerow(header)
                    header_written = True
                for row in reader:
                    writer.writerow(row)
        print("combined f1")

    with open(unique_file2, "w", newline="", encoding=out_encoding) as f2:
        writer = csv.writer(f2)
        header_written = False
        for fname in uniques_2:
            with open(fname, "r", newline="", encoding=out_encoding) as fin:
                reader = csv.reader(fin)
                header = next(reader, None)
                if not header_written and header:
                    writer.writerow(header)
                    header_written = True
                for row in reader:
                    writer.writerow(row)
        print("combined f2")

    print(small_diffs_list)
    with open(diffs, "w", newline="", encoding=out_encoding) as f3:
        writer = csv.writer(f3)
        header_written = False
        for fname in small_diffs_list:
            with open(fname, "r", newline="", encoding=out_encoding) as fin:
                reader = csv.reader(fin)
                header = next(reader, None)
                if not header_written and header:
                    writer.writerow(header)
                    header_written = True
                for row in reader:
                    writer.writerow(row)
        print("combined diffs")


def part2():
    partial1_list, partial2_list = split_excel()
    print("partial1_list:")
    print(partial1_list)
    print("partial2_list:")
    print(partial2_list)

    # # partial1_list = ['splits\\part_00001_10000.csv', 'splits\\part_10001_149302.csv'] # when debugging
    # # partial2_list = ['splits\\part_00001_10000_in_file2.csv', 'splits\\part_10001_149302_in_file2.csv'] # # when debugging

    uniques_1, uniques_2, diffs = multiple_comparisons(partial1_list, partial2_list)

    print("input to combine:")
    print(uniques_1)
    print(uniques_2)
    print(diffs)
    union_results(uniques_1, uniques_2, diffs)
    # shutil.rmtree("splits", ignore_errors=True)


def big_files_comparison():
    task_num = None
    with open(PARAMETERS_PATH, "r") as file:
        first_row = file.read()
        task_num = first_row.split(" ")[-1].strip()

    if task_num == "1":
        build_large_excel()
    elif task_num == "2":
        part2()
    else:
        print("Error: invalid task number! (need to be 1/2)")
        raise


if __name__ == "__main__":
    strarting_time = time.time()
    big_files_comparison()
    # original_task()
    ending_time = time.time()
    print("Runtime of the program is", ending_time - strarting_time)
