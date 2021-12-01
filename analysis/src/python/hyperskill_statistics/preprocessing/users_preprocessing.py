from analysis.src.python.hyperskill_statistics.common.df_utils import read_df, write_df


def get_user_level_tag(passed_problems: int):
    if passed_problems < 20:
        return 'low'
    if passed_problems > 150:
        return 'high'
    return 'avg'


def preprocess_users(users_path: str):
    df_users = read_df(users_path)
    df_users['level'] = df_users['passed_problems'].apply(get_user_level_tag)
    write_df(df_users, users_path)


if __name__ == '__main__':
    preprocess_users('../data/java/users/users.csv')
