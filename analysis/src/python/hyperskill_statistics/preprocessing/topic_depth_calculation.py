import ast
from typing import Dict, List, Tuple

import pandas as pd


def build_topics_tree(df_topics: pd.DataFrame) -> Tuple[Dict[int, List[int]], List[int]]:
    topics_tree = {topic['id']: [] for i, topic in df_topics.iterrows()}
    roots = []
    for i, topic in df_topics.iterrows():
        prerequisites = ast.literal_eval((topic['prerequisites']))
        if len(prerequisites) == 0:
            roots.append(topic['id'])
        for prerequisite in prerequisites:
            if prerequisite not in topics_tree:
                print(prerequisite)
            else:
                topics_tree[prerequisite].append(topic['id'])

    topics_tree = {topic: list(set(children)) for topic, children in topics_tree.items()}
    return topics_tree, roots


def get_topics_depth(df_topics: pd.DataFrame) -> Dict[int, int]:
    topics_tree, roots = build_topics_tree(df_topics)
    topics_tree_depth = {}
    queue = []
    for root in roots:
        topics_tree_depth[root] = 0
        queue.append(root)

    while len(queue) != 0:
        topic_id = queue.pop(0)
        for child_topic_id in topics_tree[topic_id]:
            if child_topic_id not in topics_tree_depth:
                topics_tree_depth[child_topic_id] = topics_tree_depth[topic_id] + 1
                queue.append(child_topic_id)
    return topics_tree_depth
