import pandas as pd

from analysis.src.python.hyperskill_statistics.common.df_utils import read_df, write_df
from analysis.src.python.hyperskill_statistics.common.utils import str_to_datetime
from analysis.src.python.hyperskill_statistics.model.column_name import SubmissionColumns, SubmissionColumnsStats
from analysis.src.python.hyperskill_statistics.stats_calculation.stats_utils import calc_code_rows_count, \
    calc_code_symbols_count, calc_issues_count


def calc_submission_stats(submission: pd.Series) -> pd.Series:
    submission_stats = submission[[SubmissionColumns.TIME]].copy()

    submission_stats[SubmissionColumnsStats.CODE_ROWS_COUNT] = calc_code_rows_count(
        submission[SubmissionColumns.CODE])
    submission_stats[SubmissionColumnsStats.CODE_SYMBOLS_COUNT] = calc_code_symbols_count(
        submission[SubmissionColumns.CODE])

    submission_stats[SubmissionColumnsStats.QODANA_ISSUE_COUNT] = calc_issues_count(
        submission[SubmissionColumns.QODANA_ISSUES])
    submission_stats[SubmissionColumnsStats.RAW_ISSUE_COUNT] = calc_issues_count(
        submission[SubmissionColumns.RAW_ISSUES])
    return submission_stats


def calc_submissions_series_stats(submissions: pd.DataFrame) -> pd.Series:
    submissions[SubmissionColumns.TIME] = submissions[SubmissionColumns.TIME].apply(str_to_datetime)
    submissions.sort_values([SubmissionColumns.ATTEMPT], inplace=True)

    first_submission_stats = calc_submission_stats(submissions.iloc[0])
    last_submission_stats = calc_submission_stats(submissions.iloc[-1])
    stats = submissions[[SubmissionColumns.GROUP, SubmissionColumns.ATTEMPTS]].iloc[0].copy()
    stats[SubmissionColumns.TIME] = (last_submission_stats[SubmissionColumns.TIME] -
                                     first_submission_stats[SubmissionColumns.TIME]).total_seconds()
    stats = stats.astype('int64')
    stats[SubmissionColumns.CLIENT] = submissions[[SubmissionColumns.CLIENT]] \
        .agg(list)[SubmissionColumns.CLIENT].values
    stats = pd.concat([stats,
                       first_submission_stats.add_suffix(SubmissionColumnsStats.FIRST_SUFFIX),
                       last_submission_stats.add_suffix(SubmissionColumnsStats.LAST_SUFFIX)], axis=0)
    return stats


def get_submission_statistics(submissions_with_issues_path: str, submissions_statistics_path: str):
    df_submissions = read_df(submissions_with_issues_path)

    df_submissions_series_stats = df_submissions.groupby([SubmissionColumns.GROUP]) \
        .apply(calc_submissions_series_stats)

    write_df(df_submissions_series_stats, submissions_statistics_path)


if __name__ == '__main__':
    get_submission_statistics('../data/java/filtered_submissions_with_issues_java11.csv',
                              '../data/java/filtered_submissions_series_stats_java11.csv')
