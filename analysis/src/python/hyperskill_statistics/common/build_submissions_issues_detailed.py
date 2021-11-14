import argparse
import ast
import sys
from typing import List

import pandas as pd

from analysis.src.python.hyperskill_statistics.common.df_utils import read_df, write_df
from analysis.src.python.hyperskill_statistics.common.utils import str_to_dict
from analysis.src.python.hyperskill_statistics.model.column_name import IssuesColumns, SubmissionColumns


def get_raw_issues(raw_issues, raw_issues_types):
    for raw_issue in str_to_dict(raw_issues):
        raw_issues_types[raw_issue[SubmissionColumns.RAW_ISSUE_CLASS]] = raw_issue[SubmissionColumns.RAW_ISSUE_TYPE]


def get_qodana_issues(qodana_issues, qodana_issues_types):
    for qodana_issue in str_to_dict(qodana_issues):
        qodana_issues_types[qodana_issue[SubmissionColumns.QODANA_ISSUE_CLASS]] = ''


def get_issues_types(issue_column_name: str,
                     submissions_with_issues_path: str,
                     issues_path: str):
    df_submissions = read_df(submissions_with_issues_path)
    issues_types = {}
    df_submissions[issue_column_name].apply(lambda d: get_raw_issues(d, issues_types))

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
        submissions_issues_path_detailed: str):
    df_submissions_series = read_df(submissions_with_issues_path)
    issues_list = read_df(issues_path)[IssuesColumns.CLASS].values
    issues_series = df_submissions_series.apply(
        lambda g: build_issues_series(g, issue_column_name, issue_class_column_name, issues_list), axis=1)
    write_df(issues_series, submissions_issues_path_detailed)


if __name__ == '__main__':

    parser = argparse.ArgumentParser()

    parser.add_argument('--get-issues', '-gi', type=str, help='path to submissions series', default=False)
    parser.add_argument('--submissions-series-path', '-sp', type=str, help='path to submissions series', required=True)
    parser.add_argument('--raw-issues-path', '-rp', type=str, help='path to raw issues info', required=True)
    parser.add_argument('--qodana-issues-path', '-qp', type=str, help='path to qodana issues info', required=True)
    parser.add_argument('--raw-issues-result-path', '-rrp', type=str, help='path to raw issues result', required=True)
    parser.add_argument('--qodana-issues-result-path', '-qrp', type=str, help='path to qodana issues result',
                        required=True)

    args = parser.parse_args(sys.argv[1:])
    if args.get_issues:
        get_issues_types(SubmissionColumns.RAW_ISSUES,
                         args.submissions_series_path,
                         args.raw_issues_path)
        get_issues_types(SubmissionColumns.QODANA_ISSUES,
                         args.submissions_series_path,
                         args.qodana_issues_path)

    get_solutions_with_issues_detailed(SubmissionColumns.RAW_ISSUES, SubmissionColumns.RAW_ISSUE_CLASS,
                                       args.submissions_series_path,
                                       args.raw_issues_path,
                                       args.qodana_issues_path)
    get_solutions_with_issues_detailed(SubmissionColumns.QODANA_ISSUES, SubmissionColumns.QODANA_ISSUE_CLASS,
                                       args.submissions_series_path,
                                       args.raw_issues_result_path,
                                       args.qodana_issues_result_path)
