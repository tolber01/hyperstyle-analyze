import pandas as pd
from pandarallel import pandarallel

from analysis.src.python.hyperskill_statistics.common.df_utils import write_df
from analysis.src.python.hyperskill_statistics.model.column_name import SubmissionColumns


def delete_resubmissions(group: pd.DataFrame) -> pd.DataFrame:
    i = group.shape[0] - 1
    while i > 0:
        if group.iloc[i][SubmissionColumns.CODE] == group.iloc[i - 1][SubmissionColumns.CODE]:
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
            group.drop(group.iloc[i].name, inplace=True, axis=0)
    return group


def preprocess_solutions(group: pd.DataFrame) -> pd.DataFrame:
    group = group.copy()
    group = delete_resubmissions(group)
    group = delete_strange_submissions(group)
    return group


def build_submissions_series(submissions_path: str = '../data/java/submissions_with_issues_java11.csv',
                             output_path: str = '../data/java/submissions_series_java11.csv'):
    df_submissions = pd.read_csv(submissions_path)
    pandarallel.initialize(progress_bar=True)

    df_submission_series = df_submissions.groupby([SubmissionColumns.USER_ID, SubmissionColumns.STEP_ID],
                                                  as_index=False)
    print('finish grouping')
    df_submission_series = df_submission_series.parallel_apply(lambda g: preprocess_solutions(g))
    print('finish processing')
    df_submission_series = df_submission_series.agg(list)
    print('finish aggregation')
    write_df(df_submission_series, output_path)


if __name__ == '__main__':
    build_submissions_series()
