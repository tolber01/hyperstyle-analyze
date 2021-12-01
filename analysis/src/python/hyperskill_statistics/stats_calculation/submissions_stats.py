import argparse
import logging
import sys

from analysis.src.python.hyperskill_statistics.common.df_utils import read_df, write_df
from analysis.src.python.hyperskill_statistics.model.column_name import SubmissionColumns, SubmissionColumnsStats
from analysis.src.python.hyperskill_statistics.stats_calculation.stats_utils import calc_code_rows_count, \
    calc_code_symbols_count, calc_issues_count


def get_submission_statistics(submissions_with_issues_path: str, submissions_statistics_path: str):
    df_submissions = read_df(submissions_with_issues_path)
    df_stats = df_submissions[[SubmissionColumns.ID]].copy()

    df_stats[SubmissionColumnsStats.CODE_ROWS_COUNT] = df_submissions[SubmissionColumns.CODE] \
        .apply(calc_code_rows_count)
    df_stats[SubmissionColumnsStats.CODE_SYMBOLS_COUNT] = df_submissions[SubmissionColumns.CODE] \
        .apply(calc_code_symbols_count)

    df_stats[SubmissionColumnsStats.QODANA_ISSUE_COUNT] = df_submissions[SubmissionColumns.QODANA_ISSUES] \
        .apply(calc_issues_count)
    df_stats[SubmissionColumnsStats.RAW_ISSUE_COUNT] = df_submissions[SubmissionColumns.RAW_ISSUES] \
        .apply(calc_issues_count)

    write_df(df_stats, submissions_statistics_path)


if __name__ == '__main__':
    log = logging.getLogger()
    log.setLevel(logging.DEBUG)

    parser = argparse.ArgumentParser()

    parser.add_argument('--submissions-path', '-s', type=str, help='path to submissions', required=True)
    parser.add_argument('--output-path', '-o', type=str, help='path to result file with statistics', required=True)

    args = parser.parse_args(sys.argv[1:])

    get_submission_statistics(args.submissions_path, args.output_path)
