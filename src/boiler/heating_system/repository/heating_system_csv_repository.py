import logging
import os

import pandas as pd

from ...constants import circuits_id, column_names
from boiler.parsing_utils.utils import filter_by_timestamp_closed
from .heating_system_repository import HeatingSystemRepository
from ..parsers.heating_system_data_parser import HeatingSystemDataParser
from ..interpolators.heating_system_data_interpolator import HeatingSystemDataInterpolator


class HeatingSystemCSVRepository(HeatingSystemRepository):

    FILENAME_EXT = ".csv"

    # TODO: Возможность указать кодировку csv файла
    def __init__(self,
                 circuit_id: str = circuits_id.HEATING_CIRCUIT,
                 storage_path: str = "./storage",
                 parser: HeatingSystemDataParser = None,
                 interpolator: HeatingSystemDataInterpolator = None,
                 encoding: str = 'UTF-8'):
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance of the provider")

        self._circuit_id = circuit_id
        self._storage_path = storage_path
        self._parser = parser
        self._interpolator = interpolator
        self._encoding = encoding

    def set_storage_path(self, storage_path: str):
        self._logger.debug(f"Storage path is set to {storage_path}")
        self._storage_path = storage_path

    def set_parser(self, parser: HeatingSystemDataParser):
        self._logger.debug("Parser is set")
        self._parser = parser

    def set_interpolator(self, interpolator: HeatingSystemDataInterpolator):
        self._logger.debug("Interpolator is set")
        self._interpolator = interpolator

    def set_circuit_id(self, circuit_id):
        self._logger.debug(f"Circuit id is set to {circuit_id}")
        self._circuit_id = circuit_id

    def list(self) -> list:
        self._logger.debug("Requested listing of repository")

        csv_filenames = []
        for filename_with_ext in os.listdir(self._storage_path):
            filename, ext = os.path.splitext(filename_with_ext)
            if ext == self.FILENAME_EXT:
                filepath = f"{self._storage_path}/{filename_with_ext}"
                if os.path.isfile(filepath):
                    csv_filenames.append(filename)

        return csv_filenames

    def get_dataset(self, dataset_id: str, start_datetime: pd.Timestamp = None, end_datetime: pd.Timestamp = None):
        dataset_path = os.path.abspath(f"{self._storage_path}/{dataset_id}{self.FILENAME_EXT}")
        logging.debug(f"Loading {dataset_path} from {start_datetime} to {end_datetime}")

        with open(dataset_path, encoding=self._encoding) as f:
            heating_df = self._parser.parse(f)

        heating_circuit_df = heating_df[heating_df[column_names.CIRCUIT_ID] == circuits_id.HEATING_CIRCUIT].copy()
        del heating_circuit_df[column_names.CIRCUIT_ID]

        home_heating_circuit_df = self._interpolator.interpolate_data(
            heating_circuit_df,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            inplace=True
        )

        heating_circuit_df = filter_by_timestamp_closed(
            home_heating_circuit_df,
            start_datetime,
            end_datetime
        )

        return heating_circuit_df

    def update_dataset(self, dataset_id: str, dataset: pd.DataFrame):
        raise ValueError("Operation not supported for this type of repository")

    def set_dataset(self, dataset_id: str, dataset: pd.DataFrame):
        raise ValueError("Operation not supported for this type of repository")

    def set_encoding(self, encoding):
        self._logger.debug(f"Encoding is set to {encoding}")
        self._encoding = encoding
