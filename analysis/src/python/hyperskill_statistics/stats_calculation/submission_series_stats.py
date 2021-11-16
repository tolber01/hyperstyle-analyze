import argparse
import ast
import logging
import sys

import numpy as np
import pandas as pd

from analysis.src.python.hyperskill_statistics.common.df_utils import append_df, read_df, write_df
from analysis.src.python.hyperskill_statistics.model.column_name import Client, IssuesColumns, SubmissionColumns


def series_count_statistics(issues_path: str,
                            issues_result_path: str,
                            client_count_path: str,
                            chunk_size=200):
    k = 0
    issues = read_df(issues_path)[IssuesColumns.CLASS].values

    for df_issues_result in pd.read_csv(issues_result_path, chunksize=chunk_size):
        logging.info(f"Processing chunk k={k}")
        result = {issue: [] for issue in issues}
        result[SubmissionColumns.USER_ID] = []
        result[SubmissionColumns.STEP_ID] = []
        result[Client.WEB] = []
        result[Client.IDEA] = []

        for i, issues_series in df_issues_result.iterrows():
            try:
                issues_series[SubmissionColumns.CLIENT] = ast.literal_eval(issues_series[SubmissionColumns.CLIENT])
                for issue in issues:
                    issues_series[issue] = ast.literal_eval(issues_series[issue])
            except Exception as e:
                logging.warning(f"Skipping issue {issues_series} i={i}: {e}")
                continue
            result[SubmissionColumns.USER_ID].append(issues_series[SubmissionColumns.USER_ID])
            result[SubmissionColumns.STEP_ID].append(issues_series[SubmissionColumns.STEP_ID])

            web_count = issues_series[SubmissionColumns.CLIENT].count(Client.WEB.value)
            result[Client.WEB].append(web_count)
            result[Client.IDEA].append(len(issues_series[SubmissionColumns.CLIENT]) - web_count)

            for issue in issues:
                result[issue].append(np.sum(issues_series[issue]))

        result_df = pd.DataFrame.from_dict(result)

        if k == 0:
            write_df(result_df, client_count_path)
        else:
            append_df(result_df, client_count_path)

        k += 1


if __name__ == '__main__':
    log = logging.getLogger()
    log.setLevel(logging.DEBUG)

    parser = argparse.ArgumentParser()

    parser.add_argument('--issues-path', '-i', type=str, help='path to issues', required=True)
    parser.add_argument('--issues-info-path', '-ir', type=str, help='path to issues results', required=True)
    parser.add_argument('--result-path', '-r', type=str, help='path to result file', required=True)

    args = parser.parse_args(sys.argv[1:])

    series_count_statistics(args.issues_path,
                            args.issues_info_path,
                            args.result_path)
