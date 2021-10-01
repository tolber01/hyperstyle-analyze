import argparse
from pathlib import Path
from typing import List

import pandas as pd
from src.python.review.common.file_system import get_all_file_system_items
from analysis.src.python.evaluation.common.csv_util import write_dataframe_to_csv
from analysis.src.python.evaluation.common.pandas_util import get_solutions_df_by_file_path
from analysis.src.python.evaluation.common.extensions_util import AnalysisExtension, extension_file_condition
from analysis.src.python.evaluation.common.tool_arguments_util import parse_set_arg
from analysis.src.python.evaluation.qodana.util.models import QodanaColumnName, QodanaIssue
from analysis.src.python.evaluation.qodana.util.util import to_json


def configure_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument('dataset_folder',
                        type=lambda value: Path(value).absolute(),
                        help='Path to a folder with csv files graded by Qodana. '
                             'Each file must have "inspections" column.')

    parser.add_argument('-i', '--inspections',
                        help='Set of inspections ids to exclude from the dataset',
                        type=str,
                        default='')


def __get_qodana_dataset(root: Path) -> pd.DataFrame:
    if not root.is_dir():
        raise ValueError(f'The {root} is not a directory')
    dataset_files = get_all_file_system_items(root, extension_file_condition(AnalysisExtension.CSV))
    datasets = []
    for file_path in dataset_files:
        datasets.append(get_solutions_df_by_file_path(file_path))
    return pd.concat(datasets)


def __filter_inspections(json_issues: str, inspections_to_keep: List[str]) -> str:
    issues_list = QodanaIssue.parse_list_issues_from_json(json_issues)
    filtered_issues = list(filter(lambda i: i.problem_id not in inspections_to_keep, issues_list))
    return to_json(filtered_issues)


def main() -> None:
    parser = argparse.ArgumentParser()
    configure_arguments(parser)
    args = parser.parse_args()

    dataset_folder = args.dataset_folder
    full_dataset = __get_qodana_dataset(dataset_folder)
    inspections_to_keep = parse_set_arg(args.inspections)

    full_dataset[QodanaColumnName.INSPECTIONS.value] = full_dataset.apply(
        lambda row: __filter_inspections(row[QodanaColumnName.INSPECTIONS.value], inspections_to_keep), axis=1)

    write_dataframe_to_csv(dataset_folder / f'filtered_issues{AnalysisExtension.CSV.value}', full_dataset)


if __name__ == '__main__':
    main()
