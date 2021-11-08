from enum import Enum


class StepColumns(str, Enum):
    ID = 'id'
    COMMENTS = 'comments_statistics'
    LIKES = 'likes_statistics'
    STEPIC_LESSON_ID = 'lesson_stepik_id'
    POSITION = 'position'
    SECONDS_TO_COMPLETE = 'seconds_to_complete'
    SOLVED_BY = 'solved_by'
    STAGE = 'stage'
    STEPIK_ID = 'stepik_id'
    SUCCESS_RATE = 'success_rate'
    TOPIC = 'topic'
    TOPIC_THEORY = 'topic_theory'
    TYPE = 'type'
    TITLE = 'title'
    POPULAR_IDE = 'popular_ide'
    PROJECT = 'project'
    IS_IDE_COMPATIBLE = 'is_ide_compatible'


class TopicColumns(str, Enum):
    ID = 'id'
    CHILDREN = 'children'
    DEPTH = 'depth'
    HIERARCHY = 'hierarchy'
    PREREQUISITES = 'prerequisites'
    ROOT_ID = 'root_id'
    TITLE = 'title'
    TOPOLOGICAL_INDEX = 'topological_index'
    THEORY = 'theory'
    PARENT_ID = 'parent_id'


class SubmissionColumns(str, Enum):
    ID = 'id'
    USER = 'user'
    CLIENT = 'client'
    STEP = 'step'
    CODE = 'code'
    LANG = 'lang'
    TIME = 'time'

    RAW_ISSUES = 'raw_issues'
    QODANA_ISSUES = 'qodana_issues'


class UserColumns(str, Enum):
    ID = 'id'
    COMMENTS = 'comments_posted'
    GAMIFICATION = 'gamification'

    ACTIVE_DAYS = 'active_days'
    DAILY_STEP_COMPLETED_COUNT = 'daily_step_completed_count'
    PASSED_PROBLEMS = 'passed_problems'
    PASSED_PROJECTS = 'passed_projects'
    PASSED_TOPICS = 'passed_topics'

    COMMENT = 'comment'
    HINT = 'hint'
    USEFUL_LINK = 'useful_link'
    SOLUTIONS = 'solutions'
