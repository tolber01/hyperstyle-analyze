import ast

import pandas as pd
from bs4 import BeautifulSoup
from analysis.src.python.hyperskill_statistics.common.df_utils import merge_dfs, read_df, write_df


def get_steps_complexity_tag(depth: int):
    if depth < 3:
        return 'shallow'
    if depth > 7:
        return 'deep'
    return 'middle'


def get_steps_difficulty_tag(success_rate: int):
    if success_rate < 1 / 3:
        return 'easy'
    if success_rate > 2 / 3:
        return 'hard'
    return 'medium'


def to_count(header_count_str: str):
    if str(header_count_str) == 'nan':
        return 0
    header_count_dict = ast.literal_eval(header_count_str)
    return header_count_dict.get('python3', 0)


def check_template(step: pd.DataFrame):
    return pd.Series(step['header_count'] > 0 or step['footer_count'] > 0)


def has_number_constant(step: pd.DataFrame):
    block = ast.literal_eval(step['block'])
    html = block['text']
    parsed_html = BeautifulSoup(html)
    print(parsed_html.text)
    return any(char.isdigit() for char in parsed_html.text)


def preprocess_steps(steps_path: str, topics_path: str, private_steps_path: str):
    df_steps = read_df(steps_path)
    df_topics = read_df(topics_path)
    df_private_steps = read_df(private_steps_path)
    # topics_depth = get_topics_depth(df_topics)
    #
    # df_topics['depth'] = df_topics['id'].apply(lambda topic_id: topics_depth.get(topic_id, 0))
    # df_steps['depth'] = df_steps['topic'].apply(lambda topic_id: topics_depth.get(topic_id, 0))
    # df_steps['complexity'] = df_steps['depth'].apply(get_steps_complexity_tag)
    # df_steps['difficulty'] = df_steps['success_rate'].apply(get_steps_difficulty_tag)
    df_private_steps['header_count'] = df_private_steps['header_count'].apply(to_count)
    df_private_steps['footer_count'] = df_private_steps['footer_count'].apply(to_count)
    df_steps['template'] = merge_dfs(df_steps, df_private_steps[['id', 'header_count', 'footer_count']], 'id', 'id')[
        ['header_count', 'footer_count']] \
        .apply(check_template, axis=1)
    df_steps['has_number_constant'] = df_steps[['block']].apply(has_number_constant, axis=1)
    write_df(df_topics, topics_path)
    write_df(df_steps, steps_path)


if __name__ == '__main__':
    preprocess_steps('../data/python/steps.csv', '../data/python/topics.csv', '../data/java/steps.csv')
