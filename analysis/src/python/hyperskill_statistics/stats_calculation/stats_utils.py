import ast


def calc_issues_count(issues: str) -> int:
    return len(ast.literal_eval(issues))


def calc_code_rows_count(code: str) -> int:
    return len(code.split('\n'))


def calc_code_symbols_count(code: str) -> int:
    return len(code)
