import io
import logging
from typing import List, Dict, BinaryIO, Optional

import pandas as pd
from dateutil.tz import gettz
from boiler.constants import circuit_ids
from boiler.heating_obj.io.sync.sync_heating_obj_reader import SyncHeatingObjReader
from boiler.parsing_utils.datetime_parsing import parse_datetime

from boiler_softm.constants import circuit_ids_equal as soft_m_circuit_ids_equal
from boiler_softm.constants import column_names as soft_m_column_names
from boiler_softm.constants import column_names_equal as soft_m_column_names_equals


class SoftMSyncHeatingObjCSVReader(SyncHeatingObjReader):

    def __init__(self,
                 encoding: str = "utf-8",
                 need_circuit: str = circuit_ids.HEATING_CIRCUIT,
                 timestamp_parse_patterns: Optional[List[str]] = None,
                 timestamp_timezone=None,
                 need_columns: Optional[List[str]] = None,
                 float_columns: Optional[List[str]] = None,
                 water_temp_columns: Optional[List[str]] = None) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        self._encoding = encoding

        self._timestamp_timezone = timestamp_timezone

        if timestamp_parse_patterns is None:
            timestamp_parse_patterns = (
                r"(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})\s(?P<hour>\d{2}):(?P<min>\d{2}).{7}",
                r"(?P<day>\d{2})\.(?P<month>\d{2})\.(?P<year>\d{4})\s(?P<hour>\d{1,2}):(?P<min>\d{2})"
            )
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

    def set_encoding(self, encoding: str):
        self._logger.debug(f"Encoding is set to {encoding}")
        self._encoding = encoding

    def set_timestamp_timezone(self, timezone) -> None:
        self._logger.debug(f"Timezone is set to {timezone}")
        self._timestamp_timezone = timezone

    def set_timestamp_parse_patterns(self, patterns: List[str]) -> None:
        self._logger.debug("Timestamp parse patterns is set")
        self._timestamp_parse_patterns = patterns

    def set_float_columns(self, columns: List[str]) -> None:
        self._float_columns = columns

    def set_need_circuit(self, need_circuit: str) -> None:
        self._need_circuit = need_circuit

    def set_need_columns(self, need_columns: List[str]) -> None:
        self._need_columns = need_columns

    def set_column_names_equals(self, column_names_equals: Dict[str, str]) -> None:
        self._column_names_equals = column_names_equals

    def set_circuit_id_equals(self, circuit_id_equals: Dict[str, str]) -> None:
        self._circuit_id_equals = circuit_id_equals

    def read_heating_obj_from_binary_stream(self, binary_stream: BinaryIO) -> pd.DataFrame:
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
                lambda water_temp: water_temp > 120 and water_temp / 100 or water_temp
            )
