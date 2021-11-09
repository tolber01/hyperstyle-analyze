import ast
import json
import logging

from analysis.src.python.hyperskill_statistics.common.df_utils import read_df, parallel_apply, merge_dfs, \
    rename_columns, write_df
from analysis.src.python.hyperskill_statistics.common.utils import str_to_dict
from analysis.src.python.hyperskill_statistics.model.column_name import SubmissionColumns


def qodana_issue_str_to_dict(s: str) -> str:
    return json.dumps(list(map(ast.literal_eval, str_to_dict(s)['issues'])))


def get_solutions_with_issues(submissions_path: str = '../data/java/solutions_java11.csv',
                              solutions_to_users_path: str = '../data/java/submission_to_user_anon.csv',
                              raw_issues_path: str = '../data/java/filtered_solutions_with_raw_issues.csv',
                              qodana_issues_path: str = '../data/java/labeled_filtered_solutions_java11.csv',
                              submissions_with_issues_path: str = '../data/java/submissions_with_issues_java11.csv'):
    df_submissions = read_df(submissions_path)
    logging.info(f'submissions csv initial shape: {df_submissions.shape}')

    df_solutions_to_users = read_df(solutions_to_users_path)
    df_submissions = merge_dfs(df_submissions, df_solutions_to_users, SubmissionColumns.ID, SubmissionColumns.ID)
    logging.info(f'submissions csv with user_id shape: {df_submissions.shape}')

    df_raw_issues = read_df(raw_issues_path)
    df_raw_issues = df_raw_issues[[SubmissionColumns.ID, SubmissionColumns.RAW_ISSUES, SubmissionColumns.CODE]]
    df_submissions = merge_dfs(df_submissions, df_raw_issues, SubmissionColumns.CODE, SubmissionColumns.CODE)
    logging.info(f'submissions csv with raw issues shape: {df_submissions.shape}')

    df_qodana_issues = read_df(qodana_issues_path)
    df_qodana_issues = rename_columns(df_qodana_issues, columns={'inspections': SubmissionColumns.QODANA_ISSUES})
    df_qodana_issues = parallel_apply(df_qodana_issues, SubmissionColumns.QODANA_ISSUES, qodana_issue_str_to_dict)
    df_qodana_issues = df_qodana_issues[[SubmissionColumns.ID, SubmissionColumns.QODANA_ISSUES, SubmissionColumns.CODE]]
    df_submissions = merge_dfs(df_submissions, df_qodana_issues, SubmissionColumns.CODE, SubmissionColumns.CODE)
    logging.info(f'submissions csv with qodana issues shape: {df_submissions.shape}')

    write_df(df_submissions, submissions_with_issues_path)


if __name__ == '__main__':
    get_solutions_with_issues()
