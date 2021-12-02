import argparse
import logging
import sys

import pandas as pd

from analysis.src.python.hyperskill_statistics.common.df_utils import write_df
from analysis.src.python.hyperskill_statistics.common.utils import str_to_datetime
from analysis.src.python.hyperskill_statistics.model.column_name import SubmissionColumns


# check if submission not the same as previous
def check_same_code(submission_0: pd.Series, submission_1: pd.Series) -> bool:
    return submission_0[SubmissionColumns.CODE] == submission_1[SubmissionColumns.CODE]


# check if submission not so different with previous
def check_different_code(submission_0: pd.Series, submission_1: pd.Series, diff_coef: float) -> bool:
    code_0 = submission_0[SubmissionColumns.CODE]
    code_1 = submission_1[SubmissionColumns.CODE]
    code_lines_diff = len(code_0) / len(code_1)
    return not (1 / diff_coef <= code_lines_diff <= diff_coef)


def filter_submissions_series(submissions_series: pd.DataFrame, diff_coef: float) -> pd.DataFrame:
    submissions_series = submissions_series.copy()
    submissions_series['time'] = submissions_series['time'].apply(str_to_datetime)
    submissions_series.sort_values(['time'], inplace=True)

    i = 1
    while i < submissions_series.shape[0]:

        submission_0 = submissions_series.iloc[i]
        submission_1 = submissions_series.iloc[i - 1]
        if check_same_code(submission_0, submission_1):
            logging.info(f'drop submission: '
                         f'user={submission_0[SubmissionColumns.USER_ID]} '
                         f'step={submission_0[SubmissionColumns.STEP_ID]} '
                         f'attempt={i + 1}: '
                         f'same submissions')
            submissions_series.drop(submissions_series.iloc[i].name, inplace=True, axis=0)
        elif check_different_code(submission_0, submission_1, diff_coef):
            logging.info(f'drop submission: '
                         f'user={submission_0[SubmissionColumns.USER_ID]} '
                         f'step={submission_0[SubmissionColumns.STEP_ID]} '
                         f'attempt={i + 1}: '
                         f'different submissions')
            submissions_series.drop(submissions_series.iloc[i].name, inplace=True, axis=0)
        else:
            i += 1

    size = submissions_series.shape[0]
    submissions_series.insert(2, SubmissionColumns.ATTEMPT, list(range(1, size + 1)), True)
    submissions_series.insert(2, SubmissionColumns.LAST_ATTEMPT, [size] * size, True)

    return submissions_series


def build_submission_series(submissions_path: str, output_path: str, diff_coef: float):
    df_submissions = pd.read_csv(submissions_path)
    df_submissions = df_submissions[df_submissions[SubmissionColumns.CODE].apply(lambda x: isinstance(x, str))]
    df_submissions[SubmissionColumns.GROUP] = df_submissions \
        .groupby([SubmissionColumns.USER_ID, SubmissionColumns.STEP_ID]).ngroup()

    df_submission_series = df_submissions.groupby([SubmissionColumns.GROUP], as_index=False)
    logging.info('finish grouping')
    df_filtered_submission_series = df_submission_series.apply(lambda g: filter_submissions_series(g, diff_coef))
    logging.info('finish filtering')
    df_filtered_submission = df_filtered_submission_series.reset_index(drop=True)
    logging.info('finish aggregation')
    write_df(df_filtered_submission, output_path)


if __name__ == '__main__':
    # log = logging.getLogger()
    # log.setLevel(logging.DEBUG)

    parser = argparse.ArgumentParser()

    parser.add_argument('--submissions-path', '-s', type=str, required=True,
                        help='path to submissions')
    parser.add_argument('--diff_coef', '-c', type=float, default=10.0,
                        help='if code different more in diff_coef, consider this attempt strange')
    parser.add_argument('--output-path', '-o', type=str, required=True,
                        help='path to filtered submissions')

    args = parser.parse_args(sys.argv[1:])
    build_submission_series(args.submissions_path, args.output_path, args.diff_coef)
