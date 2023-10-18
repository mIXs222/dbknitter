import enum
import os
import pandas as pd
import shutil
from functools import partial
from pathlib import Path


DEFAULT_OUTPUT_PATH = Path("./output.csv")  # TODO: Change this


class Score(str, enum.Enum):
    success = "success"
    fail_execute = "fail_execute"
    fail_missing_output = "fail_missing_output"
    fail_parsing_output = "fail_parsing_output"
    fail_wrong_output = "fail_wrong_output"


# Run target code
def execute_code(source_path: Path) -> Score:
    try:
        os.system(f"python {source_path}")
        return Score.success
    except Exception:
        return Score.fail_execute


# Move output to target name
def move_output(output_path: Path) -> Score:
    try:
        shutil.move(DEFAULT_OUTPUT_PATH, output_path)
        return Score.success
    except Exception as e:
        return Score.fail_missing_output


# Compare actual output with expected output
def compare_csv(actual_output_path: Path, expected_output_path: Path) -> Score:
    # Read actual output.
    try:
        actual_df = pd.read_csv(actual_output_path)
    except Exception as e:
        print(e)
        return Score.fail_parsing_output

    # Read expected output.
    expected_df = pd.read_csv(actual_output_path)

    # Compare outputs and return result.
    # TODO: Compare with invariants.
    print(f">>> Expected\n{expected_df}\n")
    print(f">>> Actual\n{actual_df}\n")
    correct_output = actual_df.equals(expected_df)

    # Returns.
    if not correct_output:
        return Score.fail_wrong_output
    return Score.success


# Grade one query.
def grade_one(source_path, output_path, expected_output_path) -> Score:
    score = execute_code(source_path)
    if score != Score.success:
        return score

    score = move_output(output_path)
    if score != Score.success:
        return score

    score = compare_csv(output_path, expected_output_path)
    return score


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("source_path", type=str,
                        help="Path to Python source code to grade.")
    parser.add_argument("output_path", type=str,
                        help="Path to store output CSV if successfully execute.")
    parser.add_argument("expected_output_path", type=str,
                        help="Path to expected output CSV.")
    args = parser.parse_args()
    source_path = Path(args.source_path)
    output_path = Path(args.output_path)
    expected_output_path = Path(args.expected_output_path)
    assert source_path.suffix == ".py", f"Unexpected extension: {source_path.suffix}"
    assert output_path.suffix == ".csv", f"Unexpected extension: {output_path.suffix}"
    assert expected_output_path.suffix == ".csv", f"Unexpected extension: {expected_output_path.suffix}"

    print(grade_one(source_path, output_path, expected_output_path))
