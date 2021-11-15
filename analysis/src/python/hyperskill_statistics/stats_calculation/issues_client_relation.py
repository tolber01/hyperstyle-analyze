import argparse
import ast
import sys

import numpy as np
import pandas as pd

from analysis.src.python.hyperskill_statistics.common.df_utils import read_df, write_df
from analysis.src.python.hyperskill_statistics.model.column_name import Client, IssuesColumns, SubmissionColumns


def to_list(x):
    try:
        val = ast.literal_eval(x)
    except Exception as e:
        val = np.nan
    return val


def issues_to_client(issues_path: str,
                     issues_result_path: str,
                     issues_to_client_path: str):
    issues = read_df(issues_path)[IssuesColumns.CLASS].values
    clients = [Client.WEB, Client.IDEA]
    issues_count = {issue: 0 for issue in issues}

    resolved_issues = {client: issues_count.copy() for client in clients}
    made_issues = {client: issues_count.copy() for client in clients}
    stable_issues = {client: issues_count.copy() for client in clients}

    df_issues_result = read_df(issues_result_path)

    for i, issues_series in df_issues_result.iterrows():
        try:
            for c in df_issues_result.columns:
                issues_series[c] = ast.literal_eval(issues_series[c])
        except Exception as e:
            continue

        n = len(issues_series[SubmissionColumns.TIME])
        for j in range(n):
            client = Client.WEB if Client.WEB == issues_series[SubmissionColumns.CLIENT][j] else Client.IDEA
            if j == n - 1:
                for issue in issues:
                    made_issues[client][issue] += issues_series[issue][j]
            else:
                for issue in issues:
                    issues_diff = issues_series[issue][j] - issues_series[issue][j - 1]
                    if issues_diff < 0:
                        resolved_issues[client][issue] += abs(issues_diff)
                    elif issues_diff > 0:
                        made_issues[client][issue] += abs(issues_diff)
                    else:
                        stable_issues[client][issue] += issues_series[issue][j]

    result = {'issues': issues}
    for client in clients:
        result['resolved_' + client] = [resolved_issues[client][issue] for issue in issues]
        result['made_' + client] = [made_issues[client][issue] for issue in issues]
        result['stable_' + client] = [stable_issues[client][issue] for issue in issues]
    write_df(pd.DataFrame.from_dict(result), issues_to_client_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('--issues-path', '-i', type=str, help='path to issues', required=True)
    parser.add_argument('--issues-results-path', '-ir', type=str, help='path to issues results', required=True)
    parser.add_argument('--result-path', '-r', type=str, help='path to result file', required=True)

    args = parser.parse_args(sys.argv[1:])

    issues_to_client(args.issues_path,
                     args.issues_results_path,
                     args.result_path)
