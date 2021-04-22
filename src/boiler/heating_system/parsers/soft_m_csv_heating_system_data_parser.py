import logging

import pandas as pd
from dateutil.tz import gettz

from ...constants import circuits_id, column_names
from boiler.parsing_utils.datetime_parsing import parse_datetime
from .heating_system_data_parser import HeatingSystemDataParser
from ..constants import soft_m_circuit_id_equals
from ..constants import soft_m_column_names_equals


class SoftMCSVHeatingSystemDataParser(HeatingSystemDataParser):

    def __init__(self, weather_data_timezone_name=None):
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance of the provider")

        self._timestamp_timezone_name = weather_data_timezone_name
        self._timestamp_parse_patterns = (
            r"(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})\s(?P<hour>\d{2}):(?P<min>\d{2}).{7}",
            r"(?P<day>\d{2})\.(?P<month>\d{2})\.(?P<year>\d{4})\s(?P<hour>\d{1,2}):(?P<min>\d{2})"
        )

        self._need_circuits = (circuits_id.HEATING_CIRCUIT, circuits_id.HOT_WATER_CIRCUIT)
        self._need_columns = (
            column_names.TIMESTAMP,
            column_names.CIRCUIT_ID,
            column_names.FORWARD_PIPE_COOLANT_TEMP,
            column_names.BACKWARD_PIPE_COOLANT_TEMP,
            column_names.FORWARD_PIPE_COOLANT_VOLUME,
            column_names.BACKWARD_PIPE_COOLANT_VOLUME,
            column_names.FORWARD_PIPE_COOLANT_PRESSURE,
            column_names.BACKWARD_PIPE_COOLANT_PRESSURE
        )

        self._need_to_float_convert_columns = (
            column_names.FORWARD_PIPE_COOLANT_TEMP,
            column_names.BACKWARD_PIPE_COOLANT_TEMP,
            column_names.FORWARD_PIPE_COOLANT_VOLUME,
            column_names.BACKWARD_PIPE_COOLANT_VOLUME,
            column_names.FORWARD_PIPE_COOLANT_PRESSURE,
            column_names.BACKWARD_PIPE_COOLANT_PRESSURE
        )
        self._water_temp_columns = (
            column_names.FORWARD_PIPE_COOLANT_TEMP,
            column_names.BACKWARD_PIPE_COOLANT_TEMP
        )

        self._circuit_id_equals = soft_m_circuit_id_equals.DICT
        self._column_names_equals = soft_m_column_names_equals.DICT

    def set_timestamp_timezone_name(self, timezone_name):
        self._timestamp_timezone_name = timezone_name

    def set_timestamp_parse_patterns(self, patterns):
        self._timestamp_parse_patterns = patterns

    def set_need_to_float_convert_columns(self, columns):
        self._need_to_float_convert_columns = columns

    def set_need_circuits(self, need_circuits):
        self._need_circuits = need_circuits

    def set_need_columns(self, need_columns):
        self._need_columns = need_columns

    def set_column_names_equals(self, column_names_equals):
        self._column_names_equals = column_names_equals

    def set_circuit_id_equals(self, circuit_id_equals):
        self._circuit_id_equals = circuit_id_equals

    def parse(self, data):
        self._logger.debug("Loading data")

        df = pd.read_csv(data, sep=";", low_memory=False, parse_dates=False)

        self._logger.debug("Parsing data")
        self._rename_columns(df)
        df = self._exclude_unused_columns(df)
        self._convert_circuits_id_to_str(df)
        self._rename_circuits(df)
        df = self._exclude_unused_circuits(df)
        self._parse_datetime(df)
        self._convert_values_to_float_right(df)
        self._divide_incorrect_hot_water_temp(df)

        return df

    def _rename_columns(self, df):
        self._logger.debug("Renaming columns")
        column_names_equals = {}
        for soft_m_column_name, target_column_name in self._column_names_equals.items():
            if target_column_name in self._need_columns:
                column_names_equals[soft_m_column_name] = target_column_name
        df.rename(columns=column_names_equals, inplace=True)

    def _exclude_unused_columns(self, df):
        self._logger.debug("Excluding unused columns")
        df = df[list(self._need_columns)]
        return df

    def _convert_circuits_id_to_str(self, df: pd.DataFrame):
        self._logger.debug("Converting circuits id to str")
        df[column_names.CIRCUIT_ID] = df[column_names.CIRCUIT_ID].apply(str)

    def _rename_circuits(self, df):
        self._logger.debug("Renaming circuits")
        df[column_names.CIRCUIT_ID].replace(self._circuit_id_equals, inplace=True)

    def _exclude_unused_circuits(self, df):
        self._logger.debug("Excluding unused circuits")
        df = df[df[column_names.CIRCUIT_ID].isin(self._need_circuits)]
        return df

    def _parse_datetime(self, df: pd.DataFrame):
        self._logger.debug("Parsing datetime")
        boiler_data_timezone = gettz(self._timestamp_timezone_name)
        df[column_names.TIMESTAMP] = df[column_names.TIMESTAMP].apply(
            parse_datetime, args=(self._timestamp_parse_patterns, boiler_data_timezone)
        )

    def _convert_values_to_float_right(self, df):
        self._logger.debug("Converting values to float")
        for column_name in self._need_to_float_convert_columns:
            df[column_name] = df[column_name].str.replace(",", ".", regex=False)
            df[column_name] = df[column_name].apply(float)

    def _divide_incorrect_hot_water_temp(self, df):
        self._logger.debug("Dividing incorrect water temp")
        for column_name in self._water_temp_columns:
            df[column_name] = df[column_name].apply(
                lambda water_temp: water_temp > 120 and water_temp / 100 or water_temp
            )
