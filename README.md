# Big Files Comparison (CSV/TXT) – Split, Normalize, Compare, and Merge Results

A Python tool for comparing very large CSV/TXT datasets efficiently by:
- Normalizing/standardizing two source files into comparable formats
- Splitting them into smaller chunks by a chosen key (typically an ID column)
- Running chunk-by-chunk comparisons to keep memory usage manageable
- Merging the partial comparison outputs into final consolidated result files

This project is designed for "real-world" data reconciliation tasks (e.g., HR/finance/CRM exports, migration validation, system-to-system consistency checks) where files can be too large to compare in one pass.

## What it does

- **Task 1 (prepare large comparable files)**: Creates temporary "full" normalized files for both inputs, so you can sort them by the splitting key.
- **Task 2 (split + compare + merge)**: Splits both normalized files into matching parts, compares each pair, saves per-part outputs, then merges them into final results.

You choose the task using a single parameter (`task_number`) in `parameters.txt` – no need to comment/uncomment functions.

## Quick start

### 1) Setup
- Python 3.x
- Place your input files in the project folder (or use absolute paths).
- Make sure `comparetor.py` (and its dependencies) is available in the same project.

### 2) Configure `parameters.txt`
This script reads exactly **17 space-separated values** from `parameters.txt`:

1. `file1_filename`
2. `file1_separator`
3. `file1_key_columns`
4. `mapping_1_to_2`
5. `file1_has_header`
6. `file1_date_format`
7. `file1_date_columns`
8. `file2_filename`
9. `file2_separator`
10. `file2_key_columns`
11. `file2_has_header`
12. `file2_date_format`
13. `file2_date_columns`
14. `SAP_ecoding` (or `None`)
15. `splitting_col1`
16. `splitting_col2`
17. `task_number` (`1` or `2`)

**Example:**
9206_2_source.csv , 0,1,4 (0|0) True yearmonthday 1 fixed_9206_2.csv , 0,2,6 True day/month/year 2 None 0 0 1
### 3) Run

## Workflow

### Task 1 – Build normalized "full" files (task_number = 1)

Set the last value in `parameters.txt` to `1`.

Outputs:
- For TXT input: `file1temp_full.csv`
- For CSV input: `fixed_file2temp_full.csv` (depending on your internal naming inside `Comparetor`)

Next step:
- Open both `_temp_full` files
- Sort them by the splitting key column (usually an ID)
  - Important: sort as **text** if IDs can have leading zeros
  - Make sure both files are sorted consistently

### Task 2 – Split, compare, and merge (task_number = 2)

Set the last value in `parameters.txt` to `2`.

What happens:
1. The script splits both files into matching ranges (based on `splitting_col1` and `splitting_col2`)
2. Runs comparisons for each matching pair
3. Writes outputs into:
   - `comparison_results/`

Final consolidated outputs (inside `comparison_results/`):
- `unique_in_<file1_stem>.csv`
- `unique_in_<file2_stem>.csv`
- `differences_<file1_stem>_<file2_stem>.csv`
- `unique_extras.csv` (extra rows that appear only in file2 beyond the final matched split)

## Results meaning

- **unique_in_file1**: rows that exist only in file1
- **unique_in_file2**: rows that exist only in file2
- **differences**: rows that share the same key but differ in other mapped fields

## Notes for performance and reliability

- This approach scales to large files because it avoids loading the entire comparison workload into RAM at once.
- Ensure the files are sorted consistently before Task 2, otherwise split pairing may produce incorrect comparisons.
- If you work with non-UTF8 sources, set `SAP_ecoding` appropriately (or use `None`).

## Project structure (recommended)

- `parameters.txt` – run configuration
- `comparetor.py` – comparison engine (parsing, mapping, normalization, comparison outputs)
- `comparison_results/` – generated outputs
- `splits/` – temporary chunks created during Task 2 (can be deleted after)

## Author

Yoav Knaanie
