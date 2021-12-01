from analysis.src.python.hyperskill_statistics.common.df_utils import read_df, write_df


def get_client_tag(base_client: str):
    if base_client == 'web':
        return 'web'
    return 'idea'


def preprocess_submissions(submissions_path: str):
    df_submissions = read_df(submissions_path)
    df_submissions['base_client'] = df_submissions['client']
    df_submissions['client'] = df_submissions['client'].apply(get_client_tag)
    write_df(df_submissions, submissions_path)


if __name__ == '__main__':
    preprocess_submissions('../data/java/submissions_with_series_java11.csv')
