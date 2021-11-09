from typing import List, Tuple

import pandas as pd
from pandarallel import pandarallel

from analysis.src.python.hyperskill_statistics.common.df_utils import read_df, write_df
from analysis.src.python.hyperskill_statistics.common.utils import str_to_dict
from analysis.src.python.hyperskill_statistics.model.column_name import SubmissionColumns, IssuesColumns


def get_raw_issues(raw_issues, raw_issues_types):
    for raw_issue in str_to_dict(raw_issues):
        raw_issues_types[raw_issue[SubmissionColumns.RAW_ISSUE_CLASS]] = raw_issue[SubmissionColumns.RAW_ISSUE_TYPE]


def get_qodana_issues(qodana_issues, qodana_issues_types):
    for qodana_issue in str_to_dict(qodana_issues):
        qodana_issues_types[qodana_issue[SubmissionColumns.QODANA_ISSUE_CLASS]] = ''


def get_issues_types(submissions_with_issues_path='../data/java/submissions_with_issues_java11.csv',
                     raw_issues_path='../data/java/raw_issues.csv',
                     qodana_issues_path='../data/java/qodana_issues.csv'):
    ddf_submissions = read_df(submissions_with_issues_path)

    raw_issues_types = {}
    qodana_issues_types = {}

    ddf_submissions[SubmissionColumns.RAW_ISSUES].apply(lambda d: get_raw_issues(d, raw_issues_types))
    ddf_submissions[SubmissionColumns.QODANA_ISSUES].apply(lambda d: get_qodana_issues(d, qodana_issues_types))

    write_df(pd.DataFrame.from_dict({
        IssuesColumns.CLASS: raw_issues_types.keys(),
        IssuesColumns.TYPE: raw_issues_types.values()
    }), raw_issues_path)

    write_df(pd.DataFrame.from_dict({
        IssuesColumns.CLASS: qodana_issues_types.keys(),
        IssuesColumns.TYPE: qodana_issues_types.values()
    }), qodana_issues_path)


def build_issues_series(submission_series: pd.DataFrame,
                        raw_issues_classes: List[str],
                        qodana_issues_classes: List[str]) -> Tuple[pd.Series, pd.Series]:
    first = submission_series.iloc[0]
    issues_series = {
        SubmissionColumns.USER_ID: first[SubmissionColumns.USER_ID],
        SubmissionColumns.STEP_ID: first[SubmissionColumns.STEP_ID],
        SubmissionColumns.LANG: first[SubmissionColumns.LANG],
        SubmissionColumns.CLIENT: submission_series[SubmissionColumns.CLIENT].values,
        SubmissionColumns.TIME: submission_series[SubmissionColumns.TIME].values,
        SubmissionColumns.CODE: submission_series[SubmissionColumns.CODE].values,
    }

    raw_issues_series = issues_series.copy()
    for raw_issue_class in raw_issues_classes:
        raw_issues_series[raw_issue_class] = []

    for i, raw_issues in enumerate(submission_series[SubmissionColumns.RAW_ISSUES]):

        for raw_issue_class in raw_issues_classes:
            raw_issues_series[raw_issue_class].append(0)

        for raw_issue in str_to_dict(raw_issues):
            raw_issues_series[raw_issue[SubmissionColumns.RAW_ISSUE_CLASS]][i] += 1

    qodana_issues_series = issues_series.copy()
    for qodana_issue_class in qodana_issues_classes:
        qodana_issues_series[qodana_issue_class] = []

    for i, qodana_issues in enumerate(submission_series[SubmissionColumns.QODANA_ISSUES]):

        for qodana_issue_class in qodana_issues_classes:
            qodana_issues_series[qodana_issue_class].append(0)

        for qodana_issue in str_to_dict(qodana_issues):
            qodana_issues_series[qodana_issue[SubmissionColumns.QODANA_ISSUE_CLASS]][i] += 1

    return pd.Series(raw_issues_series), pd.Series(qodana_issues_series)


def get_solutions_with_issues_detailed(
        submissions_with_issues_path: str = '../data/java/submissions_series_java11.csv',
        raw_issues_path='../data/java/raw_issues.csv',
        qodana_issues_path='../data/java/qodana_issues.csv',
        submissions_with_raw_issues_path_detailed: str = '../data/java/submissions_series_raw_issues_java11.csv',
        submissions_with_qodana_issues_path_detailed: str = '../data/java/submissions_series_qodana_issues_java11.csv'):
    pandarallel.initialize(nb_workers=4, progress_bar=True)
    df_submissions_series = read_df(submissions_with_issues_path)

    raw_issues_list = read_df(raw_issues_path)[IssuesColumns.CLASS].values
    qodana_issues_list = read_df(qodana_issues_path)[IssuesColumns.CLASS].values

    df_raw_issues_series, df_qodana_issues_series = \
        df_submissions_series.parallel_apply(lambda g: build_issues_series(g, raw_issues_list, qodana_issues_list))
    write_df(df_raw_issues_series, submissions_with_raw_issues_path_detailed)
    write_df(df_qodana_issues_series, submissions_with_qodana_issues_path_detailed)


if __name__ == '__main__':
    # get_issues_types()
    get_solutions_with_issues_detailed()
