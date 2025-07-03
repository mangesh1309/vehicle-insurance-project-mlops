import os
import sys

import numpy as np, pandas as pd
import dill
import yaml

from src.exception import CustomException
from src.logger import logger

def read_yaml_file(path: str) -> dict:
    try:
        with open(path, "rb") as yaml_file:
            return yaml.safe_load(yaml_file)
    except Exception as e:
        raise CustomException(e, sys) from e
    

def write_yaml_file(path: str, content: object, replace: bool = False) -> None:
    try:
        if replace:
            if os.path.exists(path):
                os.remove(path)

        os.makedirs(os.path.dirname(path), exist_ok=True)
        
        with open(path, "w") as file:
            yaml.dump(content, file)
    
    except Exception as e:
        raise CustomException(e, sys) from e
    

def save_object(file_path: str, obj: object) -> None:
    logger.info("Entered the save_object method of utils")

    try:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "wb") as file_obj:
            dill.dump(obj, file_obj)

        logger.info("Exited the save_object method of utils")

    except Exception as e:
        raise CustomException(e, sys) from e
    
def load_object(path: str) -> object:
    """
    Returns model/object from project directory.
    """
    try:
        with open(path, "rb") as obj:
            matter = dill.load(obj)
        return matter
    except Exception as e:
        raise CustomException(e, sys) from e

def save_numpy_array_data(file_path: str, array: np.array):
    """
    Save numpy array data to file
    file_path: str location of file to save
    array: np.array data to save
    """
    try:
        dir_path = os.path.dirname(file_path)
        os.makedirs(dir_path, exist_ok=True)
        with open(file_path, 'wb') as file_obj:
            np.save(file_obj, array)
    except Exception as e:
        raise CustomException(e, sys) from e


def load_numpy_array_data(file_path: str) -> np.array:
    """
    load numpy array data from file
    file_path: str location of file to load
    return: np.array data loaded
    """
    try:
        with open(file_path, 'rb') as file_obj:
            return np.load(file_obj)
    except Exception as e:
        raise CustomException(e, sys) from e
