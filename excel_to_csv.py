###### file 3
# -*- coding: utf-8 -*-
"""
Created on Thu Sep 18 13:36:01 2025
@author: s9082497
"""
import pandas as pd
from pathlib import Path
import os

# returns csv_path
OUTPUT_UNICODE_FORMAT = "utf-8-sig"
# writing the file in utf 8
def combine(files, outfile_path, is_excel, encoding_format):
    with open(outfile_path, "w", encoding=OUTPUT_UNICODE_FORMAT, newline="") as outfile:
        for i in range(len(files)):
            with open(files[i], "r", encoding=encoding_format) as f:
                if i>0:
                    next(f)
                for line in f:
                    line = "".join(ch for ch in line if ch.isprintable()) + "\n"
                    outfile.write(line)
            if is_excel: #otherwise no need to delete original input copy
                os.remove(files[i])
            print(f"finished {i}")
            
            
def convert_to_proper_csv(csv_path):
    # enc = from_path(csv_path).best().encoding()
    # print("detected the encoding is: " + enc)
    is_excel = True
    encoding_format = "utf-8"
    try:
        sheets = pd.read_excel(csv_path, sheet_name=None, engine="xlrd", dtype=str)
        print(f"successed open {csv_path} with:pd.read_excel(csv_path, sheet_name=None, engine=xlrd")
    except Exception:
        print("fail with xlrd not working: "+ csv_path)
        try:
            sheets = pd.read_excel(csv_path, sheet_name=None, engine="openpyxl", dtype=str)
            print(f"successed open {csv_path} with:pd.read_excel(csv_path, sheet_name=None, engine=openpyxl")
        except Exception:
            print("fail reading csv as openpyxl: " + csv_path)
            try:
                csv_df = pd.read_csv(csv_path, encoding="utf-8", dtype=str)
                is_excel = False
                print(f"successed open {csv_path} with: pd.read_csv(csv_path, encoding=utf-8")
            except Exception:
                print("fail reading csv as pd.read_csv(csv_path, sheet_name=None, encoding=utf-8: " + csv_path)
                try:
                    csv_df = pd.read_csv(csv_path, encoding="cp1255", dtype=str)
                    is_excel = False
                    print(f"successed open {csv_path} with: pd.read_csv(csv_path, encoding=cp1255")
                    encoding_format = "cp1255"
                except Exception:
                    print("fail reading csv as pd.read_csv(csv_path, sheet_name=None, encoding=cp125: " + csv_path)
                    raise
    # df.to_excel("mid.xlsx", index=False)
    # sheets = pd.read_excel("mid.csv", sheet_name=None)
               
    files_pathes = []
    if not is_excel:
        files_pathes.append(csv_path)
    else:
        # save each sheet in a diffrent file
        sheet_index = 0
        for sheet_name, df in sheets.items():
            print("sheet_name: " + sheet_name)
            sheet_index += 1
            out_path = f"{sheet_index}.csv"
            df.to_csv(out_path, index=False, encoding="utf-8")
            files_pathes.append(out_path)
            
    fixed_path = "fixed_"+csv_path
    combine(files_pathes, "fixed_"+csv_path, is_excel, encoding_format)
    return fixed_path

