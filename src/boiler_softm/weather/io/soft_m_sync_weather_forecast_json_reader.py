import io
from datetime import tzinfo
from typing import BinaryIO

import pandas as pd
from boiler.constants import column_names
from boiler.weather.io.abstract_sync_weather_reader import AbstractSyncWeatherReader

import boiler_softm.constants.converting_parameters
from boiler_softm.logging import logger

import boiler_softm.constants.column_names as soft_m_column_names
import boiler_softm.constants.processing


class SoftMSyncWeatherForecastJSONReader(AbstractSyncWeatherReader):

    def __init__(self,
                 encoding: str = "utf-8",
                 weather_data_timezone: tzinfo = None
                 ) -> None:
        self._weather_data_timezone = weather_data_timezone
        self._encoding = encoding

        self._column_names_equals = boiler_softm.constants.converting_parameters.LYSVA_WEATHER_INFO_COLUMN_EQUALS

        logger.debug(
            f"Creating instance:"
            f"weather_data_timezone: {self._weather_data_timezone}"
            f"encoding: {self._encoding}"
        )

    def read_weather_from_binary_stream(self, binary_stream: BinaryIO) -> pd.DataFrame:
        logger.debug("Parsing weather")
        with io.TextIOWrapper(binary_stream, encoding=self._encoding) as text_stream:
            df = pd.read_json(text_stream, convert_dates=False)
        self._rename_columns(df)
        self._convert_date_and_time_to_timestamp(df)
        logger.debug("Weather is parsed")
        return df

    def _rename_columns(self, df: pd.DataFrame) -> None:
        logger.debug("Renaming columns")
        df.rename(columns=self._column_names_equals, inplace=True)

    def _convert_date_and_time_to_timestamp(self, df: pd.DataFrame) -> None:
        logger.debug("Converting dates and time to timestamp")

        dates_as_str = df[soft_m_column_names.LYSVA_WEATHER_DATE]
        time_as_str = df[soft_m_column_names.LYSVA_WEATHER_TIME]
        datetime_as_str = dates_as_str.str.cat(time_as_str, sep=" ")
        timestamp = pd.to_datetime(datetime_as_str)
        timestamp = timestamp.dt.tz_localize(self._weather_data_timezone)

        df[column_names.TIMESTAMP] = timestamp
        del df[soft_m_column_names.LYSVA_WEATHER_TIME]
        del df[soft_m_column_names.LYSVA_WEATHER_DATE]
