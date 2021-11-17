import argparse
import ast
import logging
import sys
from typing import List

import pandas as pd

from analysis.src.python.hyperskill_statistics.common.df_utils import append_df, read_df, write_df
from analysis.src.python.hyperskill_statistics.model.column_name import IssuesColumns, SubmissionColumns


def calc_issues_statistics(df_submissions_with_issues: pd.DataFrame,
                           issues_classes: List[str],
                           issue_column_name: str,
                           issue_class_key: str) -> pd.DataFrame:
    issues_statistics = {
        SubmissionColumns.USER_ID: df_submissions_with_issues[SubmissionColumns.USER_ID].values,
        SubmissionColumns.STEP_ID: df_submissions_with_issues[SubmissionColumns.STEP_ID].values,
        SubmissionColumns.LANG: df_submissions_with_issues[SubmissionColumns.LANG].values,
        SubmissionColumns.CLIENT: df_submissions_with_issues[SubmissionColumns.CLIENT].values,
        SubmissionColumns.TIME: df_submissions_with_issues[SubmissionColumns.TIME].values,
        SubmissionColumns.CODE: df_submissions_with_issues[SubmissionColumns.CODE].values,
    }

    for issue_class in issues_classes:
        issues_statistics[issue_class] = []

    for i, df_submission_with_issues in df_submissions_with_issues.iterrows():
        for issue_class in issues_classes:
            issues_statistics[issue_class].append(0)
        for issue in ast.literal_eval(df_submission_with_issues[issue_column_name]):
            issues_statistics[issue[issue_class_key]][-1] += 1

    return pd.DataFrame.from_dict(issues_statistics)


def get_issues_statistics(
        submissions_with_issues_path: str,
        issues_path: str,
        issues_stats_path: str,
        issue_column_name: str,
        issue_class_key: str,
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

    parser.add_argument('--submissions-with-issues-path', '-s', type=str, help='path to issues', required=True)
    parser.add_argument('--issues-path', '-i', type=str, help='path to issues results', required=True)
    parser.add_argument('--issues-stats-path', '-is', type=str, help='path to result file', required=True)
    parser.add_argument('--issue-column-name', '-icn', type=str, help='path to result file', required=True)
    parser.add_argument('--issue-class-key', '-ick', type=str, help='path to result file', required=True)
    parser.add_argument('--chunk-size', '-cs', type=int, help='path to result file', default=20000)

    args = parser.parse_args(sys.argv[1:])

    get_issues_statistics(args.submissions_with_issues_path,
                          args.issues_path,
                          args.issues_stats_path,
                          args.issue_column_name,
                          args.issue_class_key,
                          args.chunk_size)
