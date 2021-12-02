import ast


def calc_issues_count(issues: str) -> int:
    return len(ast.literal_eval(issues))


def calc_code_rows_count(code: str) -> int:
    if isinstance(code, str):
        return len(code.split('\n'))
    return 1


def calc_code_symbols_count(code: str) -> int:
    if isinstance(code, str):
        return len(code)
    return 1
