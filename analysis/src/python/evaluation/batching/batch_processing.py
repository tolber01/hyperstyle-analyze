import argparse
import logging
import os
from collections import defaultdict
import time
from pathlib import Path
from typing import List, Tuple
import re

import numpy as np
import pandas as pd

from analysis.src.python.evaluation.common.extensions_util import AnalysisExtension
from analysis.src.python.evaluation.common.file_util import create_directory, get_name_from_path
from analysis.src.python.evaluation.common.subprocess_util import run_and_wait
from analysis.src.python.evaluation.common.yaml_util import parse_yaml

logger = logging.getLogger(__name__)


def configure_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("input", help="Path to the csv file with data to process",
                        type=lambda value: Path(value).absolute())
    parser.add_argument("output", help="Path to the output directory",
                        type=lambda value: Path(value).absolute())
    parser.add_argument("config", help="Path to the script config to run under batching",
                        type=lambda value: Path(value).absolute())
    parser.add_argument("--batch-size", help="Batch size for data", nargs='?', default=1000, type=int)
    parser.add_argument("--start-from", help="Index of batch to start processing from", nargs='?', default=0, type=int)


def run_batching():
    parser = argparse.ArgumentParser()
    configure_arguments(parser)

    args = parser.parse_args()
    batch_paths = split(args.input, args.output, args.batch_size)
    config = parse_yaml(args.config)

    for batch_index, batch_input_path, batch_logs_path, batch_output_path in batch_paths[args.start_from:]:
        start_time = time.time()
        batch_logs_file_path = os.path.join(batch_logs_path, f"log_batch_{batch_index}{AnalysisExtension.TXT.value}")
        with open(batch_logs_file_path, "w+") as fout:
            command = ["python3", config['script_path']]
            if 'script_args' in config and config['script_args'] is not None:
                for script_arg in config['script_args']:
                    command.append(script_arg)
            if 'script_flags' in config and config['script_flags'] is not None:
                for script_flag in config['script_flags'].items():
                    command += [f"-{script_flag[0]}={script_flag[1]}"]
            print(batch_input_path)
            command += [f"-i={batch_input_path}"]
            command += [f"-o={batch_output_path}"]
            run_and_wait(command, stdout=fout, stderr=fout, cwd=config['project_path'])

            end_time = time.time()
            logging.info(f"Finished batch {batch_index} processing in {end_time - start_time}s")

    merge(batch_paths, args.output)


def split(input: str, output: str, batch_size: int) -> List[Tuple[int, str, str, str]]:
    input_path = os.path.join(output, "input")
    logs_path = os.path.join(output, "logs")
    output_path = os.path.join(output, "output")

    create_directory(input_path)
    create_directory(logs_path)
    create_directory(output_path)

    df_name = get_name_from_path(input)
    df = pd.read_csv(input)
    dfs = np.array_split(df, df.size // batch_size)

    batch_paths = []
    for index, batch in enumerate(dfs):
        batch_name = f"batch_{index}"

        batch_input_path = os.path.join(input_path, batch_name)
        batch_logs_path = os.path.join(logs_path, batch_name)
        batch_output_path = os.path.join(output_path, batch_name)

        create_directory(batch_input_path)
        create_directory(batch_logs_path)
        create_directory(batch_output_path)

        batch_input_file = os.path.join(batch_input_path, df_name)
        batch.to_csv(batch_input_file)

        batch_paths.append((index, batch_input_file, batch_logs_path, batch_output_path))
        logging.info(f"Create {index} batch")

    return batch_paths


def merge(batch_paths: List[Tuple[int, str, str, str]], output: str):
    output_files_by_name = defaultdict(list)
    for _, _, _, batch_output_path in batch_paths:
        output_files = os.listdir(batch_output_path)
        for output_file in output_files:
            if AnalysisExtension.get_extension_from_file(output_file) == AnalysisExtension.CSV:
                output_file_id = re.sub(r"\.*batch_[0-9]_+\.*", "", get_name_from_path(output_file))
                output_files_by_name[output_file_id].append(os.path.join(batch_output_path, output_file))
    for output_file_name, output_files in output_files_by_name.items():
        dfs = []
        for output_file in output_files:
            dfs.append(pd.read_csv(output_file))
        result = pd.concat(dfs)
        result.to_csv(os.path.join(output, output_file_name))


if __name__ == "__main__":
    run_batching()
