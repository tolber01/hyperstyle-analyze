import argparse
import ast
import logging
import sys
from typing import List

import pandas as pd

from analysis.src.python.hyperskill_statistics.common.df_utils import append_df, read_df, write_df
from analysis.src.python.hyperskill_statistics.model.column_name import IssuesColumns, SubmissionColumns


def calc_issues_statistics(df_submissions: pd.DataFrame,
                           issues_classes: List[str],
                           issue_column_name: str,
                           issue_class_key: str) -> pd.DataFrame:
    issues_statistics = {
        SubmissionColumns.ID: df_submissions[SubmissionColumns.ID].values,
    }

    for issue_class in issues_classes:
        issues_statistics[issue_class] = []

    for i, submission_with_issues in df_submissions.iterrows():
        for issue_class in issues_classes:
            issues_statistics[issue_class].append(0)
        for issue in ast.literal_eval(submission_with_issues[issue_column_name]):
            issues_statistics[issue[issue_class_key]][-1] += 1

    return pd.DataFrame.from_dict(issues_statistics)


def get_issues_statistics(
        issue_class_key: str,
        issue_column_name: str,
        submissions_with_issues_path: str,
        issues_path: str,
        issues_stats_path: str,
        chunk_size):
    issues_list = read_df(issues_path)[IssuesColumns.CLASS].values

    logging.info(f"Processing dataframe chunk_size={chunk_size}")
    k = 0
    for df_submissions_with_issues in pd.read_csv(submissions_with_issues_path, chunksize=chunk_size):
        logging.info(f"Processing chunk={k}")
        df_issues_stats = calc_issues_statistics(df_submissions_with_issues, issues_list,
                                                 issue_column_name, issue_class_key)
        if k == 0:
            write_df(df_issues_stats, issues_stats_path)
        else:
            append_df(df_issues_stats, issues_stats_path)
        k += 1


if __name__ == '__main__':
    log = logging.getLogger()
    log.setLevel(logging.DEBUG)

    parser = argparse.ArgumentParser()

    parser.add_argument('--submissions-path', '-s', type=str, help='path to submissions series', required=True)
    parser.add_argument('--issues-type', '-t', type=str, help='type of issues to analyse',
                        choices=[SubmissionColumns.RAW_ISSUES, SubmissionColumns.QODANA_ISSUES])
    parser.add_argument('--issues-path', '-p', type=str, help='path to issues info', required=True)
    parser.add_argument('--output', '-o', type=str, help='path to issues submissions detailed result', required=True)
    parser.add_argument('--chunk-size', '-c', type=int, help='path to submissions series', default=50000)

    args = parser.parse_args(sys.argv[1:])

    issues_type = SubmissionColumns(args.issues_type)
    if issues_type == SubmissionColumns.QODANA_ISSUES:
        issue_class_key = SubmissionColumns.QODANA_ISSUE_CLASS
        issue_type_column_name = SubmissionColumns.QODANA_ISSUE_TYPE
    else:
        issue_class_key = SubmissionColumns.RAW_ISSUE_CLASS
        issue_type_column_name = SubmissionColumns.RAW_ISSUE_TYPE

    get_issues_statistics(issue_class_key, issues_type,
                          args.submissions_path,
                          args.issues_path,
                          args.output,
                          args.chunk_size)
