import ast
import json
from datetime import datetime


def str_to_dict(s):
    return json.loads(s)


def str_to_datetime(s):
    return datetime.fromisoformat(s)


def qodana_issue_str_to_dict(s: str) -> str:
    return json.dumps(list(map(ast.literal_eval, str_to_dict(s)['issues'])))
