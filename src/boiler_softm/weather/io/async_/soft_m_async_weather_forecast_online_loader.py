import asyncio
import io
import logging
from typing import Optional

import aiohttp
import pandas as pd
from boiler.parsing_utils.utils import filter_by_timestamp_closed
from boiler.weather.io.async_.async_weather_loader import AsyncWeatherLoader
from boiler.weather.io.sync.sync_weather_reader import SyncWeatherReader


class SoftMAsyncWeatherForecastOnlineLoader(AsyncWeatherLoader):

    def __init__(self,
                 server_address: str = "https://lysva.agt.town/",
                 reader: SyncWeatherReader = None) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance of the provider")

        self._weather_data_server_address = server_address
        self._weather_reader = reader

    def set_server_address(self, server_address: str) -> None:
        self._logger.debug(f"Server address is set to {server_address}")
        self._weather_data_server_address = server_address

    def set_weather_reader(self, weather_reader: SyncWeatherReader) -> None:
        self._logger.debug("Weather reader is set")
        self._weather_reader = weather_reader

    async def load_weather(self,
                           start_datetime: Optional[pd.Timestamp] = None,
                           end_datetime: Optional[pd.Timestamp] = None) -> pd.DataFrame:
        self._logger.debug(f"Requested weather forecast from {start_datetime} to {end_datetime}")
        raw_weather_forecast = await self._get_forecast_from_server()
        weather_df = await self._read_weather_forecast(raw_weather_forecast)
        weather_df = filter_by_timestamp_closed(weather_df, start_datetime, end_datetime)
        self._logger.debug(f"Gathered {len(weather_df)} weather forecast items")
        return weather_df

    async def _get_forecast_from_server(self) -> bytes:
        self._logger.debug(f"Requesting weather forecast from server {self._weather_data_server_address}")

        url = f"{self._weather_data_server_address}/JSON/"
        # noinspection SpellCheckingInspection
        params = {
            "method": "getPrognozT"
        }
        async with aiohttp.request("GET", url, params=params) as response:
            raw_response = await response.read()
            self._logger.debug(f"Weather forecast is loaded. Response status code is {response.status}")

        return raw_response

    async def _read_weather_forecast(self, raw_weather_forecast: bytes) -> pd.DataFrame:
        self._logger.debug("Reading weather forecast in executor")
        loop = asyncio.get_running_loop()
        with io.BytesIO(raw_weather_forecast) as binary_stream:
            weather_forecast_df = await loop.run_in_executor(
                None,
                self._weather_reader.read_weather_from_binary_stream,
                binary_stream
            )
        self._logger.debug("Weather forecast is read")

        return weather_forecast_df
