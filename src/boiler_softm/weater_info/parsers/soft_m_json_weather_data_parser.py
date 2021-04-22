import logging

import pandas as pd
from dateutil import tz
from boiler.weater_info.parsers.weather_data_parser import WeatherDataParser
from boiler.constants import column_names

import boiler_softm.constants.column_names as soft_m_column_names
from boiler_softm.constants import column_names_equals


class SoftMJSONWeatherDataParser(WeatherDataParser):

    def __init__(self, weather_data_timezone=tz.UTC):
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance of the provider")

        self._weather_data_timezone = weather_data_timezone
        self._column_names_equals = column_names_equals.WEATHER_INFO_COLUMN_EQUALS

    def set_weather_data_timezone(self, timezone):
        self._logger.debug(f"Weather timezone is set to {timezone}")
        self._weather_data_timezone = timezone

    def set_column_names_equals(self, names_equals):
        self._logger.debug("Column names equals are set")
        self._column_names_equals = names_equals

    def parse_weather_data(self, weather_data):
        self._logger.debug("Parsing weather data")

        df = pd.read_json(weather_data, convert_dates=False)
        self._rename_columns(df)
        self._convert_date_and_time_to_timestamp(df)

        return df

    def _rename_columns(self, df):
        self._logger.debug("Renaming columns")
        df.rename(columns=self._column_names_equals, inplace=True)

    def _convert_date_and_time_to_timestamp(self, df):
        self._logger.debug("Converting dates and time to timestamp")

        dates_as_str = df[soft_m_column_names.WEATHER_DATE]
        time_as_str = df[soft_m_column_names.WEATHER_TIME]
        datetime_as_str = dates_as_str.str.cat(time_as_str, sep=" ")
        timestamp = pd.to_datetime(datetime_as_str)
        timestamp = timestamp.dt.tz_localize(self._weather_data_timezone)

        df[column_names.TIMESTAMP] = timestamp
        del df[soft_m_column_names.WEATHER_TIME]
        del df[soft_m_column_names.WEATHER_DATE]
