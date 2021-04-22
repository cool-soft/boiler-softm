import logging

import aiohttp
import pandas as pd

from boiler.constants import column_names
from boiler.weater_info.repository.weather_repository import WeatherRepository
from boiler.weater_info.parsers.weather_data_parser import WeatherDataParser
from boiler.weater_info.interpolators.weather_data_interpolator import WeatherDataInterpolator


class OnlineSoftMWeatherForecastRepository(WeatherRepository):

    def __init__(self,
                 server_address="https://lysva.agt.town/",
                 weather_data_parser: WeatherDataParser = None,
                 weather_data_interpolator: WeatherDataInterpolator = None):
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance of the provider")

        self._weather_data_server_address = server_address
        self._weather_data_parser = weather_data_parser
        self._weather_data_interpolator = weather_data_interpolator

    def set_server_address(self, server_address):
        self._logger.debug(f"Server address is set to {server_address}")
        self._weather_data_server_address = server_address

    def set_weather_data_parser(self, weather_data_parser: WeatherDataParser):
        self._logger.debug("Weather data parser is set")
        self._weather_data_parser = weather_data_parser

    async def get_weather_info(self, start_datetime: pd.Timestamp = None, end_datetime: pd.Timestamp = None):
        self._logger.debug(f"Requested weather info from {start_datetime} to {end_datetime}")

        data = await self._get_forecast_from_server()
        weather_df = self._weather_data_parser.parse_weather_data(data)
        weather_df = self._weather_data_interpolator.interpolate_weather_data(weather_df)

        if start_datetime is not None:
            weather_df = weather_df[weather_df[column_names.TIMESTAMP] >= start_datetime]
        if end_datetime is not None:
            weather_df = weather_df[weather_df[column_names.TIMESTAMP] <= end_datetime]

        self._logger.debug(f"Gathered {len(weather_df)} weather info items")

        return weather_df

    async def _get_forecast_from_server(self):
        self._logger.debug(f"Requesting weather forecast from server {self._weather_data_server_address}")

        url = f"{self._weather_data_server_address}/JSON/"
        # noinspection SpellCheckingInspection
        params = {
            "method": "getPrognozT"
        }
        async with aiohttp.request("GET", url, params=params) as response:
            response_text = await response.text()
            self._logger.debug(f"Weather forecast is loaded. Response status code is {response.status}")

        return response_text

    async def set_weather_info(self, weather_df: pd.DataFrame):
        raise ValueError("This operationis not supported for this repository type")

    async def update_weather_info(self, weather_df: pd.DataFrame):
        raise ValueError("This operationis not supported for this repository type")

    async def delete_weather_info_older_than(self, datetime: pd.Timestamp):
        raise ValueError("This operationis not supported for this repository type")
