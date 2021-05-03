import io
import logging
from typing import List, BinaryIO

import pandas as pd
from boiler.constants import circuit_ids, column_names
from boiler.data_processing.other import parse_datetime
from boiler.heating_obj.io.abstract_sync_heating_obj_reader import AbstractSyncHeatingObjReader

from boiler_softm.constants import circuit_ids_equal as soft_m_circuit_ids_equal
from boiler_softm.constants import column_names as soft_m_column_names
from boiler_softm.constants import column_names_equal as soft_m_column_names_equals


class SoftMSyncHeatingObjCSVReader(AbstractSyncHeatingObjReader):

    def __init__(self,
                 timestamp_parse_patterns: List[str],
                 timestamp_timezone,
                 need_columns: List[str],
                 float_columns: List[str],
                 water_temp_columns: List[str],
                 encoding: str = "utf-8",
                 need_circuit: str = circuit_ids.HEATING_CIRCUIT,
                 ) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        self._encoding = encoding
        self._timestamp_timezone = timestamp_timezone
        self._timestamp_parse_patterns = timestamp_parse_patterns
        self._need_circuit = need_circuit
        self._need_columns = need_columns
        self._float_columns = float_columns
        self._water_temp_columns = water_temp_columns

        self._circuit_id_equals = soft_m_circuit_ids_equal.CIRCUIT_ID_EQUALS
        self._column_names_equals = soft_m_column_names_equals.HEATING_OBJ_COLUMN_NAMES_EQUALS

    def read_heating_obj_from_binary_stream(self,
                                            binary_stream: BinaryIO
                                            ) -> pd.DataFrame:
        self._logger.debug("Loading")

        with io.TextIOWrapper(binary_stream, encoding=self._encoding) as text_stream:
            df = pd.read_csv(text_stream, sep=";", low_memory=False, parse_dates=False)

        self._logger.debug("Parsing")
        df = self._convert_circuit_ids(df)
        df = self._exclude_unused_circuits(df)
        df = self._rename_equal_columns(df)
        df = self._parse_timestamp(df)
        df = self._convert_values_to_float(df)
        df = self._divide_incorrect_hot_water_temp(df)
        df = self._exclude_unused_columns(df)

        return df

    def _rename_equal_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        self._logger.debug("Renaming equal columns")
        df = df.copy()
        column_names_equals = {}
        for soft_m_column_name, target_column_name in self._column_names_equals.items():
            if target_column_name in self._need_columns:
                column_names_equals[soft_m_column_name] = target_column_name
        df = df.rename(columns=column_names_equals)
        return df

    def _convert_circuit_ids(self, df: pd.DataFrame) -> pd.DataFrame:
        self._logger.debug("Converting circuit ids")
        df = df.copy()
        df[column_names.CIRCUIT_ID] = \
            df[soft_m_column_names.HEATING_SYSTEM_CIRCUIT_ID].apply(str).replace(self._circuit_id_equals)
        return df

    def _exclude_unused_circuits(self, df: pd.DataFrame) -> pd.DataFrame:
        self._logger.debug("Excluding unused circuits")
        df = df[df[column_names.CIRCUIT_ID] == self._need_circuit].copy()
        return df

    def _parse_timestamp(self, df: pd.DataFrame) -> pd.DataFrame:
        self._logger.debug("Parsing datetime")
        df = df.copy()
        df[column_names.TIMESTAMP] = df[soft_m_column_names.HEATING_SYSTEM_TIMESTAMP].apply(
            parse_datetime, args=(self._timestamp_parse_patterns, self._timestamp_timezone)
        )
        return df

    def _convert_values_to_float(self, df: pd.DataFrame) -> pd.DataFrame:
        self._logger.debug("Converting values to float")
        df = df.copy()
        for column_name in self._float_columns:
            df[column_name] = df[column_name].str.replace(",", ".", regex=False)
            df[column_name] = df[column_name].apply(float)
        return df

    def _divide_incorrect_hot_water_temp(self, df: pd.DataFrame) -> pd.DataFrame:
        self._logger.debug("Dividing incorrect water temp")
        df = df.copy()
        for column_name in self._water_temp_columns:
            df[column_name] = df[column_name].apply(
                lambda water_temp: water_temp > 100 and water_temp / 100 or water_temp
            )
        return df

    def _exclude_unused_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        self._logger.debug("Excluding unused columns")
        df = df[list(self._need_columns)].copy()
        return df
