import os
import pickle
import shutil
from pathlib import Path
from typing import Union, Any

from src.python.review.common.file_system import Extension

from analysis.src.python.evaluation.common.extensions_util import AnalysisExtension


def create_directory(path: str):
    if not os.path.exists(path):
        os.makedirs(path)


# File should contain the full path and its extension.
# Create all parents if necessary
def create_file(file_path: Union[str, Path], content: str):
    file_path = Path(file_path)

    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, 'w+') as f:
        f.writelines(content)
        yield Path(file_path)


# For getting name of the last folder or file
# For example, returns 'folder' for both 'path/data/folder' and 'path/data/folder/'
def get_name_from_path(path: Union[Path, str], with_extension: bool = True) -> str:
    head, tail = os.path.split(path)
    # Tail can be empty if '/' is at the end of the path
    file_name = tail or os.path.basename(head)
    if not with_extension:
        file_name = os.path.splitext(file_name)[0]
    elif AnalysisExtension.get_extension_from_file(file_name) == Extension.EMPTY:
        raise ValueError('Cannot get file name with extension, because the passed path does not contain it')
    return file_name


def copy_file(source: Union[str, Path], destination: Union[str, Path]):
    shutil.copy(source, destination)


def copy_directory(source: Union[str, Path], destination: Union[str, Path], dirs_exist_ok: bool = True):
    shutil.copytree(source, destination, dirs_exist_ok=dirs_exist_ok)


def get_parent_folder(path: Union[Path, str], to_add_slash: bool = False) -> Path:
    path = remove_slash(str(path))
    parent_folder = '/'.join(path.split('/')[:-1])
    if to_add_slash:
        parent_folder = add_slash(parent_folder)
    return Path(parent_folder)


def add_slash(path: str) -> str:
    if not path.endswith('/'):
        path += '/'
    return path


def remove_slash(path: str) -> str:
    return path.rstrip('/')


def remove_directory(directory: Union[str, Path]) -> None:
    if os.path.isdir(directory):
        shutil.rmtree(directory, ignore_errors=True)


def serialize_data_and_write_to_file(path: Path, data: Any) -> None:
    os.makedirs(get_parent_folder(path), exist_ok=True)
    with open(path, 'wb') as f:
        p = pickle.Pickler(f)
        p.dump(data)


def deserialize_data_from_file(path: Path) -> Any:
    with open(path, 'rb') as f:
        u = pickle.Unpickler(f)
        return u.load()
