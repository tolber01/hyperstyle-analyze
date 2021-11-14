import ast
from typing import List

import pandas as pd
from pandarallel import pandarallel

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
                     submissions_with_issues_path='../data/java/submissions_with_issues_java11.csv',
                     issues_path='../data/java/type_issues.csv'):
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
        submissions_with_issues_path: str = '../data/java/submissions_series_java11.csv',
        issues_path='../data/java/type_issues.csv',
        submissions_issues_path_detailed: str = '../data/java/type_issues_series_java11.csv'):

    pandarallel.initialize(progress_bar=True)
    df_submissions_series = read_df(submissions_with_issues_path)
    issues_list = read_df(issues_path)[IssuesColumns.CLASS].values
    issues_series = df_submissions_series.parallel_apply(
        lambda g: build_issues_series(g, issue_column_name, issue_class_column_name, issues_list),
        axis=1)
    write_df(issues_series, submissions_issues_path_detailed)


if __name__ == '__main__':
    # get_issues_types(SubmissionColumns.RAW_ISSUES, issues_path='../data/java/raw_issues.csv')
    # get_issues_types(SubmissionColumns.QODANA_ISSUES, issues_path='../data/java/qodana_issues.csv')
    get_solutions_with_issues_detailed(SubmissionColumns.RAW_ISSUES, SubmissionColumns.RAW_ISSUE_CLASS,
                                       issues_path='../data/java/raw_issues.csv',
                                       submissions_issues_path_detailed='../data/java/raw_issues_series_java11.csv')
    get_solutions_with_issues_detailed(SubmissionColumns.QODANA_ISSUES, SubmissionColumns.QODANA_ISSUE_CLASS,
                                       issues_path='../data/java/qodana_issues.csv',
                                       submissions_issues_path_detailed='../data/java/qodana_issues_series_java11.csv')
