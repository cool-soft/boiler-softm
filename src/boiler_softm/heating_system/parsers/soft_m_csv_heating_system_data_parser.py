import logging

import pandas as pd
from boiler.heating_system.parsers.heating_system_data_parser import HeatingSystemDataParser
from boiler.parsing_utils.datetime_parsing import parse_datetime
from dateutil.tz import gettz

from boiler_softm.constants import column_names_equals as softm_column_names_equals
from boiler_softm.constants import circuit_id_equals as softm_circuit_id_equals
from boiler_softm.constants import circuits_id as softm_circuit_ids
from boiler_softm.constants import column_names as softm_column_names


class SoftMCSVHeatingSystemDataParser(HeatingSystemDataParser):

    def __init__(self, data_timezone_name=None):
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance of the provider")

        self._timestamp_timezone_name = data_timezone_name
        self._timestamp_parse_patterns = (
            r"(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})\s(?P<hour>\d{2}):(?P<min>\d{2}).{7}",
            r"(?P<day>\d{2})\.(?P<month>\d{2})\.(?P<year>\d{4})\s(?P<hour>\d{1,2}):(?P<min>\d{2})"
        )

        self._need_circuits = (softm_circuit_ids.HEATING_CIRCUIT, softm_circuit_ids.HOT_WATER_CIRCUIT)
        self._need_columns = (
            softm_column_names.HEATING_SYSTEM_TIMESTAMP,
            softm_column_names.HEATING_SYSTEM_CIRCUIT_ID,
            softm_column_names.HEATING_SYSTEM_FORWARD_PIPE_COOLANT_TEMP,
            softm_column_names.HEATING_SYSTEM_BACKWARD_PIPE_COOLANT_TEMP,
            softm_column_names.HEATING_SYSTEM_FORWARD_PIPE_COOLANT_VOLUME,
            softm_column_names.HEATING_SYSTEM_BACKWARD_PIPE_COOLANT_VOLUME,
            softm_column_names.HEATING_SYSTEM_FORWARD_PIPE_COOLANT_PRESSURE,
            softm_column_names.HEATING_SYSTEM_BACKWARD_PIPE_COOLANT_PRESSURE
        )

        self._need_to_float_convert_columns = (
            softm_column_names.HEATING_SYSTEM_FORWARD_PIPE_COOLANT_TEMP,
            softm_column_names.HEATING_SYSTEM_BACKWARD_PIPE_COOLANT_TEMP,
            softm_column_names.HEATING_SYSTEM_FORWARD_PIPE_COOLANT_VOLUME,
            softm_column_names.HEATING_SYSTEM_BACKWARD_PIPE_COOLANT_VOLUME,
            softm_column_names.HEATING_SYSTEM_FORWARD_PIPE_COOLANT_PRESSURE,
            softm_column_names.HEATING_SYSTEM_BACKWARD_PIPE_COOLANT_PRESSURE
        )
        self._water_temp_columns = (
            softm_column_names.HEATING_SYSTEM_FORWARD_PIPE_COOLANT_TEMP,
            softm_column_names.HEATING_SYSTEM_BACKWARD_PIPE_COOLANT_TEMP
        )

        self._circuit_id_equals = softm_circuit_id_equals.CIRCUIT_ID_EQUALS
        self._column_names_equals = softm_column_names_equals.HEATING_SYSTEM_COLUMN_NAMES_EQUALS

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
        df[softm_column_names.HEATING_SYSTEM_CIRCUIT_ID] = df[softm_column_names.HEATING_SYSTEM_CIRCUIT_ID].apply(str)

    def _rename_circuits(self, df):
        self._logger.debug("Renaming circuits")
        df[softm_column_names.HEATING_SYSTEM_CIRCUIT_ID].replace(self._circuit_id_equals, inplace=True)

    def _exclude_unused_circuits(self, df):
        self._logger.debug("Excluding unused circuits")
        df = df[df[softm_column_names.HEATING_SYSTEM_CIRCUIT_ID].isin(self._need_circuits)]
        return df

    def _parse_datetime(self, df: pd.DataFrame):
        self._logger.debug("Parsing datetime")
        boiler_data_timezone = gettz(self._timestamp_timezone_name)
        df[softm_column_names.HEATING_SYSTEM_TIMESTAMP] = df[softm_column_names.HEATING_SYSTEM_TIMESTAMP].apply(
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
