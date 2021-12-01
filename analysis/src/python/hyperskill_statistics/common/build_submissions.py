import argparse
import json
import logging
import sys

import pandas as pd

from analysis.src.python.hyperskill_statistics.common.df_utils import merge_dfs, parallel_apply, read_df, \
    rename_columns, write_df
from analysis.src.python.hyperskill_statistics.common.utils import qodana_issue_str_to_dict
from analysis.src.python.hyperskill_statistics.model.column_name import SubmissionColumns


def merge_submissions_with_issues(df_submissions: pd.Dataframe, df_issues: pd.Dataframe, issue_columns: str):
    df_issues = df_issues[[SubmissionColumns.ID, SubmissionColumns.RAW_ISSUES, SubmissionColumns.CODE]]
    df_submissions = merge_dfs(df_submissions, df_issues,
                               SubmissionColumns.CODE, SubmissionColumns.CODE, how='left')
    df_submissions[issue_columns] = df_submissions[issue_columns].fillna(value=json.dumps([]))
    return df_submissions


def build_submissions_dataframe(submissions_path: str,
                                solutions_to_users_path: str,
                                raw_issues_path: str,
                                qodana_issues_path: str,
                                submissions_with_issues_path: str):
    df_submissions = read_df(submissions_path)
    logging.info(f'submissions csv initial shape: {df_submissions.shape}')

    df_solutions_to_users = read_df(solutions_to_users_path)
    df_submissions = merge_dfs(df_submissions, df_solutions_to_users, SubmissionColumns.ID, SubmissionColumns.ID)
    logging.info(f'submissions csv with user_id shape: {df_submissions.shape}')

    df_raw_issues = read_df(raw_issues_path)
    df_submissions = merge_submissions_with_issues(df_submissions, df_raw_issues, SubmissionColumns.RAW_ISSUES)
    logging.info(f'submissions csv with raw issues shape: {df_submissions.shape}')

    df_qodana_issues = read_df(qodana_issues_path)
    df_qodana_issues = rename_columns(df_qodana_issues, columns={'inspections': SubmissionColumns.QODANA_ISSUES})
    df_qodana_issues = parallel_apply(df_qodana_issues, SubmissionColumns.QODANA_ISSUES, qodana_issue_str_to_dict)
    df_submissions = merge_submissions_with_issues(df_submissions, df_qodana_issues, SubmissionColumns.QODANA_ISSUES)
    logging.info(f'submissions csv with qodana issues shape: {df_submissions.shape}')

    write_df(df_submissions, submissions_with_issues_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('--solutions', '-s', type=str, help='path to file with solutions')
    parser.add_argument('--user', '-u', type=str, help='path to file with user/solution relation')
    parser.add_argument('--raw-issues', '-ri', type=str, help='path to file with raw issues/solution relation')
    parser.add_argument('--qodana-issues', '-qi', type=str, help='path to file with qodana issues/solution relation')
    parser.add_argument('--output', '-o', type=str, help='path to output file with submissions')

    args = parser.parse_args(sys.argv[1:])

    build_submissions_dataframe(args.submissions, args.user, args.raw_issues, args.qodana_issues, args.result)
