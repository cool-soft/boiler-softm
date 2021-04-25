import logging
import os
from typing import List

import pandas as pd
from boiler.parsing_utils.utils import filter_by_timestamp_closed
from boiler.heating_system.repository.stream.sync.heating_system_stream_sync_repository \
    import HeatingSystemStreamSyncRepository
from boiler.heating_system.parsers.heating_system_data_parser import HeatingSystemDataParser
from boiler.heating_system.interpolators.heating_system_data_interpolator import HeatingSystemDataInterpolator
from boiler.constants import circuit_ids, column_names


class SoftMHeatingSystemStreamSyncCSVRepository(HeatingSystemStreamSyncRepository):
    FILENAME_EXT = ".csv"

    def __init__(self,
                 circuit_id: str = circuit_ids.HEATING_CIRCUIT,
                 storage_path: str = "./storage",
                 parser: HeatingSystemDataParser = None,
                 interpolator: HeatingSystemDataInterpolator = None,
                 encoding: str = 'UTF-8') -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance of the provider")

        self._circuit_id = circuit_id
        self._storage_path = storage_path
        self._parser = parser
        self._interpolator = interpolator
        self._encoding = encoding

    def set_storage_path(self, storage_path: str) -> None:
        self._logger.debug(f"Storage path is set to {storage_path}")
        self._storage_path = storage_path

    def set_parser(self, parser: HeatingSystemDataParser) -> None:
        self._logger.debug("Parser is set")
        self._parser = parser

    def set_interpolator(self, interpolator: HeatingSystemDataInterpolator) -> None:
        self._logger.debug("Interpolator is set")
        self._interpolator = interpolator

    def set_circuit_id(self, circuit_id: str) -> None:
        self._logger.debug(f"Circuit id is set to {circuit_id}")
        self._circuit_id = circuit_id

    def set_encoding(self, encoding: str) -> None:
        self._logger.debug(f"Encoding is set to {encoding}")
        self._encoding = encoding

    def list(self) -> List[str]:
        self._logger.debug("Requested listing of io")

        csv_filenames = []
        for filename_with_ext in os.listdir(self._storage_path):
            filename, ext = os.path.splitext(filename_with_ext)
            if ext == self.FILENAME_EXT:
                filepath = f"{self._storage_path}/{filename_with_ext}"
                if os.path.isfile(filepath):
                    csv_filenames.append(filename)

        return csv_filenames

    def get_dataset(self,
                    dataset_id: str,
                    start_datetime: pd.Timestamp = None,
                    end_datetime: pd.Timestamp = None) -> pd.DataFrame:
        dataset_path = os.path.abspath(f"{self._storage_path}/{dataset_id}{self.FILENAME_EXT}")
        logging.debug(f"Loading {dataset_path} from {start_datetime} to {end_datetime}")

        with open(dataset_path, encoding=self._encoding) as f:
            heating_df = self._parser.parse(f)

        heating_circuit_df = heating_df[heating_df[column_names.CIRCUIT_ID] ==
                                        circuit_ids.HEATING_CIRCUIT].copy()
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

    def update_dataset(self, dataset_id: str, dataset: pd.DataFrame) -> None:
        raise ValueError("Operation not supported for this type of io")

    def set_dataset(self, dataset_id: str, dataset: pd.DataFrame) -> None:
        raise ValueError("Operation not supported for this type of io")
