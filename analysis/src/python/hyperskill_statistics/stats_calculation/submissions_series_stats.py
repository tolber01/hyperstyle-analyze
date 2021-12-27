import json
import logging

import pandas as pd

from analysis.src.python.hyperskill_statistics.common.df_utils import append_df, read_df, write_df
from analysis.src.python.hyperskill_statistics.model.column_name import SubmissionColumns


def calc_submissions_client_series(series: pd.DataFrame) -> pd.Series:
    series = series.sort_values([SubmissionColumns.ATTEMPT])
    stats = {
        SubmissionColumns.GROUP: series[SubmissionColumns.GROUP].values[0],
        SubmissionColumns.CLIENT: json.dumps(list(series[SubmissionColumns.CLIENT].values))
    }

    return pd.Series(stats)


def get_submission_statistics(submissions_with_issues_path: str,
                              output_path: str,
                              chunk_size: int):
    df_submissions = pd.read_csv(submissions_with_issues_path)

    min_group, max_group = df_submissions[SubmissionColumns.GROUP].min(), df_submissions[SubmissionColumns.GROUP].max()
    logging.info(f'groups range: [{min_group}, {max_group}]')

    for i in range(min_group, max_group + 1, chunk_size):
        logging.info(f'processing groups: [{i}, {i + chunk_size})')
        df_filtered_submission_series = df_submissions[(df_submissions[SubmissionColumns.GROUP] >= i) &
                                                       (df_submissions[SubmissionColumns.GROUP] < i + chunk_size)]
        df_grouped_submission_series = df_filtered_submission_series.groupby([SubmissionColumns.GROUP], as_index=False)
        logging.info('finish grouping')
        df_client_series = df_grouped_submission_series.apply(calc_submissions_client_series)
        logging.info('finish filtering')
        df_client_series = df_client_series.reset_index(drop=True)
        logging.info('finish aggregation')
        if i == 0:
            write_df(df_client_series, output_path)
        else:
            append_df(df_client_series, output_path)


if __name__ == '__main__':
    log = logging.getLogger()
    log.setLevel(logging.DEBUG)
    get_submission_statistics('../data/python/result_solutions_python_with_series_full.csv',
                              '../data/python/result_submissions_client_stats_python.csv',
                              50000)
