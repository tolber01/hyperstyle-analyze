import argparse
import sys

import pandas as pd
from pandarallel import pandarallel

from analysis.src.python.hyperskill_statistics.model.column_name import SubmissionColumns


def build_series(group, code_difference_coef: float = 2.0, ):
    group.sort_by(SubmissionColumns.TIME, in)


def build_submissions_series(submissions_path: str):
    pandarallel.initialize()
    df_submissions = pd.read_csv(submissions_path)
    df_submissions \
        .groupby([SubmissionColumns.USER, SubmissionColumns.STEP]) \
        .parallel_apply(build_series)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--submissions-path', '-sbf', type=str, help='path to csv with submissions', required=True)
    args = parser.parse_args(sys.argv[1:])
