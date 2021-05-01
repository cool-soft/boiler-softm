import io
import logging
from typing import List, BinaryIO

import pandas as pd
from boiler.constants import circuit_ids
from boiler.data_processing.other import parse_datetime
from boiler.heating_obj.io.abstract_sync_heating_obj_reader import AbstractSyncHeatingObjReader
from dateutil.tz import gettz

from boiler_softm.constants import circuit_ids_equal as soft_m_circuit_ids_equal, parsing_patterns
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

        if timestamp_parse_patterns is None:
            timestamp_parse_patterns = parsing_patterns.HEATING_OBJ_TIMESTAMP
        self._timestamp_parse_patterns = timestamp_parse_patterns

        self._need_circuit = need_circuit

        if need_columns is None:
            need_columns = (
                soft_m_column_names.HEATING_SYSTEM_TIMESTAMP,
                soft_m_column_names.HEATING_SYSTEM_CIRCUIT_ID,
                soft_m_column_names.HEATING_SYSTEM_FORWARD_PIPE_COOLANT_TEMP,
                soft_m_column_names.HEATING_SYSTEM_BACKWARD_PIPE_COOLANT_TEMP,
                soft_m_column_names.HEATING_SYSTEM_FORWARD_PIPE_COOLANT_VOLUME,
                soft_m_column_names.HEATING_SYSTEM_BACKWARD_PIPE_COOLANT_VOLUME,
                soft_m_column_names.HEATING_SYSTEM_FORWARD_PIPE_COOLANT_PRESSURE,
                soft_m_column_names.HEATING_SYSTEM_BACKWARD_PIPE_COOLANT_PRESSURE
            )
        self._need_columns = need_columns

        if float_columns is None:
            float_columns = (
                soft_m_column_names.HEATING_SYSTEM_FORWARD_PIPE_COOLANT_TEMP,
                soft_m_column_names.HEATING_SYSTEM_BACKWARD_PIPE_COOLANT_TEMP,
                soft_m_column_names.HEATING_SYSTEM_FORWARD_PIPE_COOLANT_VOLUME,
                soft_m_column_names.HEATING_SYSTEM_BACKWARD_PIPE_COOLANT_VOLUME,
                soft_m_column_names.HEATING_SYSTEM_FORWARD_PIPE_COOLANT_PRESSURE,
                soft_m_column_names.HEATING_SYSTEM_BACKWARD_PIPE_COOLANT_PRESSURE
            )
        self._float_columns = float_columns

        if water_temp_columns is None:
            water_temp_columns = (
                soft_m_column_names.HEATING_SYSTEM_FORWARD_PIPE_COOLANT_TEMP,
                soft_m_column_names.HEATING_SYSTEM_BACKWARD_PIPE_COOLANT_TEMP
            )
        self._water_temp_columns = water_temp_columns

        self._circuit_id_equals = soft_m_circuit_ids_equal.CIRCUIT_ID_EQUALS
        self._column_names_equals = soft_m_column_names_equals.HEATING_SYSTEM_COLUMN_NAMES_EQUALS

    def read_heating_obj_from_binary_stream(self,
                                            binary_stream: BinaryIO
                                            ) -> pd.DataFrame:
        self._logger.debug("Loading text_stream")

        with io.TextIOWrapper(binary_stream, encoding=self._encoding) as text_stream:
            df = pd.read_csv(text_stream, sep=";", low_memory=False, parse_dates=False)

        self._logger.debug("Parsing text_stream")
        self._rename_columns(df)
        df = self._exclude_unused_columns(df)
        self._convert_circuits_id_to_str(df)
        self._rename_circuits(df)
        df = self._exclude_unused_circuits(df)
        self._parse_datetime(df)
        self._convert_values_to_float(df)
        self._divide_incorrect_hot_water_temp(df)

        return df

    def _rename_columns(self, df: pd.DataFrame) -> None:
        self._logger.debug("Renaming columns")
        column_names_equals = {}
        for soft_m_column_name, target_column_name in self._column_names_equals.items():
            if target_column_name in self._need_columns:
                column_names_equals[soft_m_column_name] = target_column_name
        df.rename(columns=column_names_equals, inplace=True)

    def _exclude_unused_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        self._logger.debug("Excluding unused columns")
        df = df[list(self._need_columns)]
        return df

    def _convert_circuits_id_to_str(self, df: pd.DataFrame) -> None:
        self._logger.debug("Converting circuits id to str")
        df[soft_m_column_names.HEATING_SYSTEM_CIRCUIT_ID] = df[soft_m_column_names.HEATING_SYSTEM_CIRCUIT_ID].apply(str)

    def _rename_circuits(self, df: pd.DataFrame) -> None:
        self._logger.debug("Renaming circuits")
        df[soft_m_column_names.HEATING_SYSTEM_CIRCUIT_ID].replace(self._circuit_id_equals, inplace=True)

    def _exclude_unused_circuits(self, df: pd.DataFrame) -> pd.DataFrame:
        self._logger.debug("Excluding unused circuits")
        df = df[df[soft_m_column_names.HEATING_SYSTEM_CIRCUIT_ID] == self._need_circuit]
        return df

    def _parse_datetime(self, df: pd.DataFrame) -> None:
        self._logger.debug("Parsing datetime")
        boiler_data_timezone = gettz(self._timestamp_timezone)
        df[soft_m_column_names.HEATING_SYSTEM_TIMESTAMP] = df[soft_m_column_names.HEATING_SYSTEM_TIMESTAMP].apply(
            parse_datetime, args=(self._timestamp_parse_patterns, boiler_data_timezone)
        )

    def _convert_values_to_float(self, df: pd.DataFrame) -> None:
        self._logger.debug("Converting values to float")
        for column_name in self._float_columns:
            df[column_name] = df[column_name].str.replace(",", ".", regex=False)
            df[column_name] = df[column_name].apply(float)

    def _divide_incorrect_hot_water_temp(self, df: pd.DataFrame) -> None:
        self._logger.debug("Dividing incorrect water temp")
        for column_name in self._water_temp_columns:
            df[column_name] = df[column_name].apply(
                lambda water_temp: water_temp > 100 and water_temp / 100 or water_temp
            )
