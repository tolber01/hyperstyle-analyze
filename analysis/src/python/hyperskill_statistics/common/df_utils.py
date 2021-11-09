import os
from typing import List, Dict, Callable

import pandas as pd
from pandarallel import pandarallel


def _apply_to_raw(raw: pd.Series, column: str, func: Callable) -> pd.Series:
    copy_row = raw.copy()
    copy_row[column] = func(copy_row[column])
    return copy_row


def apply(df: pd.DataFrame, column: str, func):
    return df.apply(lambda raw: _apply_to_raw(raw, column, func), axis=1)


def parallel_apply(df: pd.DataFrame, column: str, func: Callable):
    pandarallel.initialize(nb_workers=4, progress_bar=True)
    return df.parallel_apply(lambda raw: _apply_to_raw(raw, column, func), axis=1)


def rename_columns(df: pd.DataFrame, columns: Dict[str, str]) -> pd.DataFrame:
    return df.rename(columns=columns)


def drop_columns(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    return df.drop(labels=columns, axis=1)


def merge_dfs(df_left: pd.DataFrame, df_right: pd.DataFrame, left_on: str, right_on: str) -> pd.DataFrame:
    df_merged = pd.merge(df_left, df_right, left_on=left_on, right_on=right_on, suffixes=('', '_extra'))
    df_merged.drop(df_merged.filter(regex='_extra$').columns.tolist(), axis=1, inplace=True)
    return df_merged


def read_df(path: str) -> pd.DataFrame:
    return pd.read_csv(path)


def write_df(df: pd.DataFrame, path: str):
    df.to_csv(path, index=False)
