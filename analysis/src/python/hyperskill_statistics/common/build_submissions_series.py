import sys

import argparse
import pandas as pd

from analysis.src.python.hyperskill_statistics.common.df_utils import write_df
from analysis.src.python.hyperskill_statistics.model.column_name import SubmissionColumns


def delete_resubmissions(group: pd.DataFrame) -> pd.DataFrame:
    i = group.shape[0] - 1
    while i > 0:
        if group.iloc[i][SubmissionColumns.CODE] == group.iloc[i - 1][SubmissionColumns.CODE]:
            print(f'drop submission '
                  f'user={group.iloc[i][SubmissionColumns.USER_ID]} '
                  f'step={group.iloc[i][SubmissionColumns.STEP_ID]} '
                  f'attempt={i}: '
                  f'same code')
            i -= 1
        else:
            break
    return group[:i + 1]


def delete_strange_submissions(group: pd.DataFrame, coef: float = 5.0) -> pd.DataFrame:
    i = 1
    while i < group.shape[0]:
        c = len(group.iloc[i][SubmissionColumns.CODE]) / len(group.iloc[i - 1][SubmissionColumns.CODE])
        if 1 / coef <= c <= coef:
            i += 1
        else:
            print(f'drop submission: '
                  f'user={group.iloc[i][SubmissionColumns.USER_ID]} '
                  f'step={group.iloc[i][SubmissionColumns.STEP_ID]} '
                  f'attempt={i}: '
                  f'number of lines diff coef = {c}')
            group.drop(group.iloc[i].name, inplace=True, axis=0)
    return group


def preprocess_solutions(group: pd.DataFrame) -> pd.DataFrame:
    group = group.copy()
    group = delete_resubmissions(group)
    group = delete_strange_submissions(group)
    return group


def build_submissions_series(submissions_path: str, output_path: str):
    df_submissions = pd.read_csv(submissions_path)
    df_submission_series = df_submissions.groupby([SubmissionColumns.USER_ID, SubmissionColumns.STEP_ID],
                                                  as_index=False)
    print('finish grouping')
    df_submission_series = df_submission_series.apply(lambda g: preprocess_solutions(g))
    print('finish processing')
    df_submission_series = df_submission_series.agg(list)
    print('finish aggregation')
    write_df(df_submission_series, output_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('--submissions-path', '-sp', type=str, help='path to submissions', required=True)
    parser.add_argument('--submissions-series-path', '-sp', type=str, help='path to submissions series', required=True)

    args = parser.parse_args(sys.argv[1:])
    build_submissions_series(args.submissions_path, args.submissions_series_path)
