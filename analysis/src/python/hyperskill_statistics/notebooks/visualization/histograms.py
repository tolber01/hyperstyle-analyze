from typing import List

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots


def show_count_histograms(df: pd.DataFrame,
                          xs: List[str],
                          title: str,
                          yaxis_title: str,
                          yaxes_type: str = "-",
                          cols: int = 2):
    rows, cols = int(np.ceil(len(xs) / 2.0)), min(len(xs), cols)
    fig = make_subplots(rows=rows, cols=cols, subplot_titles=xs)

    for i, x in enumerate(xs):
        row, col = (i // 2) + 1, i % 2 + 1
        fig.add_trace(go.Histogram(x=df[x], name=x), row=row, col=col)
        fig.update_xaxes(title_text=f'{x} count', row=row, col=col)
        fig.update_yaxes(type=yaxes_type, row=row, col=col)

    fig.update_layout(
        title=title,
        yaxis_title=yaxis_title,
        height=400 * rows
    )
    fig.show()


def show_relation_histograms(df: pd.DataFrame,
                             xs: List[str],
                             ys: List[str],
                             cs: List[str],
                             title: str,
                             yaxis_title: str,
                             yaxes_type: str = "-",
                             cols: int = 2):
    rows, cols = int(np.ceil(len(xs) / 2.0)), min(len(xs), cols)
    fig = make_subplots(rows=rows, cols=cols, subplot_titles=xs)

    for i, xy in enumerate(zip(xs, ys, cs)):
        x, y = xy
        row, col = (i // 2) + 1, i % 2 + 1
        fig.add_trace(go.Histogram(x=df[x], name=x), row=row, col=col)
        fig.update_xaxes(title_text=f'{x} count', row=row, col=col)
        fig.update_yaxes(type=yaxes_type, row=row, col=col)

    fig.update_layout(
        title=title,
        yaxis_title=yaxis_title,
        height=400 * rows
    )
    fig.show()


def show_side_by_side_comparison_histograms(df: pd.DataFrame, attributes: List[str], divider_attribute: str, title: str,
                                            yaxis_title: str, yaxes_type: str = '-'):
    rows, cols = int(np.ceil(len(attributes) / 2.0)), min(len(attributes), 2)
    fig = make_subplots(rows=rows, cols=cols, subplot_titles=attributes)

    X = df[divider_attribute].unique()
    D = [df[df[divider_attribute] == x] for x in X]
    for i, attribute in enumerate(attributes):
        row, col = (i // 2) + 1, i % 2 + 1

        for attribute_value in df[attribute].unique():
            Y = []
            for d in D:
                Y.append(d[d[attribute] == attribute_value].shape[0] / d.shape[0] * 100)
            fig.add_trace(go.Bar(x=X, y=Y, name=attribute_value), row=row, col=col)
        fig.update_xaxes(title_text=divider_attribute, row=row, col=col)
    fig.update_yaxes(type=yaxes_type)

    fig.update_layout(
        title=title,
        yaxis_title=yaxis_title,
        barmode='stack'
    )
    fig.show()
