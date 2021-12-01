from analysis.src.python.hyperskill_statistics.common.df_utils import read_df, write_df
from analysis.src.python.hyperskill_statistics.preprocessing.topic_depth_calculation import get_topics_depth


def get_steps_complexity_tag(depth: int):
    if depth < 3:
        return 'shallow'
    if depth > 5:
        return 'deep'
    return 'middle'


def get_steps_difficulty_tag(success_rate: int):
    if success_rate < 1 / 3:
        return 'easy'
    if success_rate > 2 / 3:
        return 'hard'
    return 'medium'


def preprocess_steps(steps_path: str, topics_path: str):
    df_steps = read_df(steps_path)
    df_topics = read_df(topics_path)
    topics_depth = get_topics_depth(df_topics)

    df_topics['depth'] = df_topics['id'].apply(lambda topic_id: topics_depth.get(topic_id, 0))
    df_steps['depth'] = df_steps['topic'].apply(lambda topic_id: topics_depth.get(topic_id, 0))
    df_steps['complexity'] = df_steps['depth'].apply(get_steps_complexity_tag)
    df_steps['difficulty'] = df_steps['success_rate'].apply(get_steps_difficulty_tag)
    write_df(df_topics, topics_path)
    write_df(df_steps, steps_path)


if __name__ == '__main__':
    preprocess_steps('../data/java/client/steps.csv', '../../data/java/client/topics.csv')
