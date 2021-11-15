import argparse
import ast
import logging
import sys
from typing import List

import pandas as pd

from analysis.src.python.hyperskill_statistics.common.df_utils import append_df, read_df, write_df
from analysis.src.python.hyperskill_statistics.common.utils import str_to_dict
from analysis.src.python.hyperskill_statistics.model.column_name import IssuesColumns, SubmissionColumns


def get_issues(issues, issue_class_column_name: str, issue_type_column_name: str, issues_types):
    for issue in str_to_dict(issues):
        issues_types[issue[issue_type_column_name]] = issue.get(issue_class_column_name, '')


def get_issues_classes(issue_column_name: str,
                       issue_class_column_name: str,
                       issue_type_column_name: str,
                       submissions_with_issues_path: str,
                       issues_path: str):
    df_submissions = read_df(submissions_with_issues_path)
    issues_types = {}
    df_submissions[issue_column_name].apply(
        lambda d: get_issues(d, issue_class_column_name, issue_type_column_name, issues_types))

    write_df(pd.DataFrame.from_dict({
        IssuesColumns.CLASS: issues_types.keys(),
        IssuesColumns.TYPE: issues_types.values()
    }), issues_path)


def build_issues_series(submission_series: pd.Series,
                        issue_column_name: str,
                        issue_class_column_name: str,
                        issues_classes: List[str]) -> pd.Series:
    issues_series = {
        SubmissionColumns.USER_ID: submission_series[SubmissionColumns.USER_ID],
        SubmissionColumns.STEP_ID: submission_series[SubmissionColumns.STEP_ID],
        SubmissionColumns.LANG: submission_series[SubmissionColumns.LANG],
        SubmissionColumns.CLIENT: submission_series[SubmissionColumns.CLIENT],
        SubmissionColumns.TIME: submission_series[SubmissionColumns.TIME],
        SubmissionColumns.CODE: submission_series[SubmissionColumns.CODE],
    }

    for issue_class in issues_classes:
        issues_series[issue_class] = []

    for i, issues in enumerate(ast.literal_eval(submission_series[issue_column_name])):
        for issue_class in issues_classes:
            issues_series[issue_class].append(0)
        for raw_issue in str_to_dict(issues):
            issues_series[raw_issue[issue_class_column_name]][i] += 1

    return pd.Series(issues_series)


def get_solutions_with_issues_detailed(
        issue_column_name: str,
        issue_class_column_name: str,
        submissions_with_issues_path: str,
        issues_path: str,
        submissions_issues_path_detailed: str,
        chunk_size=20000):
    df_submissions_series = read_df(submissions_with_issues_path)
    issues_list = read_df(issues_path)[IssuesColumns.CLASS].values

    size = df_submissions_series.shape[0]
    logging.info(f"Processing dataframe size={size} chunk_size={chunk_size}")
    for i in range(0, size // chunk_size + 1):
        logging.info(f"Processing chunk={i}, slice={(i * chunk_size, (i + 1) * chunk_size)}")
        issues_series = df_submissions_series[i * chunk_size:(i + 1) * chunk_size].apply(
            lambda g: build_issues_series(g, issue_column_name, issue_class_column_name, issues_list), axis=1)
        if i == 0:
            logging.info(f"Writing chunk {i}")
            write_df(issues_series, submissions_issues_path_detailed)
        else:
            logging.info(f"Appending chunk {i}")
            append_df(issues_series, submissions_issues_path_detailed)


if __name__ == '__main__':
    log = logging.getLogger()
    log.setLevel(logging.DEBUG)

    parser = argparse.ArgumentParser()

    parser.add_argument('--issues-type', '-t', type=str, help='type of issues to analyse',
                        choices=[SubmissionColumns.RAW_ISSUES, SubmissionColumns.QODANA_ISSUES])
    parser.add_argument('--get-issues', '-i', type=str, help='path to submissions series', default=False)
    parser.add_argument('--submissions-series-path', '-sp', type=str, help='path to submissions series', required=True)
    parser.add_argument('--issues-path', '-p', type=str, help='path to issues info', required=True)
    parser.add_argument('--issues-result-path', '-r', type=str, help='path to issues result', required=True)

    args = parser.parse_args(sys.argv[1:])

    issues_type = SubmissionColumns(args.issues_type)
    if issues_type == SubmissionColumns.QODANA_ISSUES:
        issue_class_column_name = SubmissionColumns.QODANA_ISSUE_CLASS
        issue_type_column_name = SubmissionColumns.QODANA_ISSUE_TYPE
    else:
        issue_class_column_name = SubmissionColumns.RAW_ISSUE_CLASS
        issue_type_column_name = SubmissionColumns.RAW_ISSUE_TYPE

    if args.get_issues:
        logging.info("Getting issue classes")
        get_issues_classes(issues_type, issue_class_column_name, issue_type_column_name,
                           args.submissions_series_path,
                           args.issues_path)

    logging.info("Getting issue default info")
    get_solutions_with_issues_detailed(issues_type, issue_class_column_name,
                                       args.submissions_series_path,
                                       args.issues_path,
                                       args.issues_result_path)
