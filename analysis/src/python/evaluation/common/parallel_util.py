from multiprocessing import Pool
from typing import TypeVar, List, Callable, Any

R = TypeVar('R')


def run_parallel(input: List[Any], f: Callable[[Any], R]) -> List[R]:
    with Pool() as p:
        result = p.starmap(f, input)
    return result
