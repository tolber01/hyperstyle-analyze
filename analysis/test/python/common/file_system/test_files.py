import re
from pathlib import Path
from typing import List, Tuple, Union

from src.python.review.common.file_system import Extension, get_all_file_system_items

from analysis.src.python.evaluation.common.extensions_util import AnalysisExtension, match_condition


def pair_in_and_out_files(in_files: List[Path], out_files: List[Path]) -> List[Tuple[Path, Path]]:
    pairs = []
    for in_file in in_files:
        out_file = Path(re.sub(r'in(?=[^in]*$)', 'out', str(in_file)))
        if out_file not in out_files:
            raise ValueError(f'List of out files does not contain a file for {in_file}')
        pairs.append((in_file, out_file))
    return pairs


def get_in_and_out_list(root: Path,
                        in_ext: Union[Extension, AnalysisExtension] = AnalysisExtension.CSV,
                        out_ext: Union[Extension, AnalysisExtension]
                        = AnalysisExtension.CSV) -> List[Tuple[Path, Path]]:
    in_files = get_all_file_system_items(root, match_condition(rf'in_\d+{in_ext.value}'))
    out_files = get_all_file_system_items(root, match_condition(rf'out_\d+{out_ext.value}'))
    return pair_in_and_out_files(in_files, out_files)