import logging
import os

import pandas as pd

from boiler.parsing_utils.utils import filter_by_timestamp_closed
from .heating_system_repository import HeatingSystemRepository


class HeatingSystemPickleRepository(HeatingSystemRepository):

    FILENAME_EXT = ".pickle"

    def __init__(self, storage_path: str = "./storage"):
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance of the provider")

        self._storage_path = storage_path

    def set_storage_path(self, storage_path: str):
        self._logger.debug(f"Storage path is set to {storage_path}")
        self._storage_path = storage_path

    def list(self) -> list:
        self._logger.debug("Requested listing of repository")

        pickle_filenames = []
        for filename_with_ext in os.listdir(self._storage_path):
            filename, ext = os.path.splitext(filename_with_ext)
            if ext == self.FILENAME_EXT:
                filepath = f"{self._storage_path}/{filename_with_ext}"
                if os.path.isfile(filepath):
                    pickle_filenames.append(filename)

        return pickle_filenames

    def get_dataset(self, dataset_id: str, start_datetime: pd.Timestamp = None, end_datetime: pd.Timestamp = None):
        dataset_path = os.path.abspath(f"{self._storage_path}/{dataset_id}{self.FILENAME_EXT}")
        logging.debug(f"Loading {dataset_path} from {start_datetime} to {end_datetime}")

        df = pd.read_pickle(dataset_path)
        filter_by_timestamp_closed(df, start_datetime, end_datetime)

        return df

    def set_dataset(self, dataset_id: str, dataset: pd.DataFrame):
        dataset_path = os.path.abspath(f"{self._storage_path}/{dataset_id}{self.FILENAME_EXT}")
        logging.debug(f"Saving {dataset_path}")

        dataset.to_pickle(dataset_path)

    def update_dataset(self, dataset_id: str, dataset: pd.DataFrame):
        raise ValueError("Operation not supported for this type of repository")
