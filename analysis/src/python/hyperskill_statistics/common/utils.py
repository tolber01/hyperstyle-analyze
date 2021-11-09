import json
from datetime import datetime


def str_to_dict(s):
    return json.loads(s)


def str_to_datetime(s):
    return datetime.fromisoformat(s)
