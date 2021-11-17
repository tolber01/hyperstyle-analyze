import argparse
import logging
import sys

import pandas as pd

from analysis.src.python.hyperskill_statistics.common.df_utils import write_df
from analysis.src.python.hyperskill_statistics.model.column_name import SubmissionColumns


def filter_resubmissions(submissions_series: pd.DataFrame) -> pd.DataFrame:
    i = submissions_series.shape[0] - 1
    while i > 0:
        if submissions_series.iloc[i][SubmissionColumns.CODE] == submissions_series.iloc[i - 1][SubmissionColumns.CODE]:
            logging.info(f'drop submission '
                         f'user={submissions_series.iloc[i][SubmissionColumns.USER_ID]} '
                         f'step={submissions_series.iloc[i][SubmissionColumns.STEP_ID]} '
                         f'attempt={i}: '
                         f'same code')
            i -= 1
        else:
            break
    return submissions_series[:i + 1]


def filter_strange_submissions(submissions_series: pd.DataFrame, diff_coef: float) -> pd.DataFrame:
    i = 1
    while i < submissions_series.shape[0]:
        code_len = len(submissions_series.iloc[i][SubmissionColumns.CODE]) / \
                   len(submissions_series.iloc[i - 1][SubmissionColumns.CODE])
        if 1 / diff_coef <= code_len <= diff_coef:
            i += 1
        else:
            logging.info(f'drop submission: '
                         f'user={submissions_series.iloc[i][SubmissionColumns.USER_ID]} '
                         f'step={submissions_series.iloc[i][SubmissionColumns.STEP_ID]} '
                         f'attempt={i}: '
                         f'number of lines diff coef={code_len}')
            submissions_series.drop(submissions_series.iloc[i].name, inplace=True, axis=0)
    return submissions_series


def filter_submissions_series(submissions_series: pd.DataFrame, diff_coef: float) -> pd.DataFrame:
    submissions_series = submissions_series.copy()
    submissions_series = filter_resubmissions(submissions_series)
    submissions_series = filter_strange_submissions(submissions_series, diff_coef)
    return submissions_series


def filter_submissions(submissions_path: str, output_path: str, diff_coef: float):
    df_submissions = pd.read_csv(submissions_path)
    df_submission_series = df_submissions.groupby([SubmissionColumns.USER_ID, SubmissionColumns.STEP_ID],
                                                  as_index=False)
    logging.info('finish grouping')
    df_filtered_submission_series = df_submission_series.apply(lambda g: filter_submissions_series(g, diff_coef))
    logging.info('finish filtering')
    df_filtered_submission = df_filtered_submission_series.reset_index(drop=True)
    logging.info('finish aggregation')
    write_df(df_filtered_submission, output_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('--submissions-path', '-s', type=str, required=True,
                        help='path to submissions')
    parser.add_argument('--filtered-submissions-path', '-fs', type=str, required=True,
                        help='path to filtered submissions')
    parser.add_argument('--diff_coef', '-c', type=float, default=10.0,
                        help='if code different more in diff_coef, consider this attempt strange')

    args = parser.parse_args(sys.argv[1:])
    filter_submissions(args.submissions_path, args.filtered_submissions_path, args.diff_coef)
