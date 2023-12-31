import enum
import pandas as pd
import shutil
import subprocess

# from cdifflib import CSequenceMatcher
# SequenceMatcher = CSequenceMatcher

from difflib import SequenceMatcher
from functools import partial
from pathlib import Path

pd.set_option('display.max_columns', None)
pd.set_option('display.expand_frame_repr', False)


def str_similar(a, b):
    return SequenceMatcher(None, a, b).ratio()


DEFAULT_OUTPUT_PATH = Path("./query_output.csv")  # TODO: Change this


class Score(str, enum.Enum):
    success = "success"
    success_approx = "success_approx"
    fail_execute = "fail_execute"
    fail_timeout = "fail_timeout"
    fail_missing_output = "fail_missing_output"
    fail_parsing_output = "fail_parsing_output"
    fail_wrong_output = "fail_wrong_output"


# Run target code
def execute_code(source_path: Path) -> Score:
    try:
        subprocess.run(["python", f"{source_path}"], check=True, timeout=600)  # 10 minutes
        return Score.success
    except subprocess.CalledProcessError:
        return Score.fail_execute
    except subprocess.TimeoutExpired:
        return Score.fail_timeout


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
    expected_df = pd.read_csv(expected_output_path)

    # Remove invariants.
    actual_df.sort_index(axis=1, inplace=True)
    expected_df.sort_index(axis=1, inplace=True)

    # Compare outputs and return result.
    print(f">>> Expected ({expected_df.shape})\n{expected_df}\n")
    print(f">>> Actual ({actual_df.shape})\n{actual_df}\n")
    correct_output = actual_df.equals(expected_df)
    if not correct_output and expected_df.shape == actual_df.shape:
        # Compare string similarity
        # similarity = str_similar(actual_df.to_string(), expected_df.to_string())
        similarity = str_similar(actual_df.head(100).to_string(), expected_df.head(100).to_string())
        print(f"Head row similarity: {similarity}")
        if similarity > 0.7:
            return Score.success_approx
        # print(f"difflib similarity: {similarity}")
        # correct_output = (str(actual_df) == str(expected_df))
        # print(f"Override with string comparison")

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
