import asyncio
import io
from concurrent.futures.thread import ThreadPoolExecutor
from typing import Optional, Union

import aiohttp
import pandas as pd
from boiler.data_processing.beetween_filter_algorithm \
    import AbstractTimestampFilterAlgorithm, LeftClosedTimestampFilterAlgorithm
from boiler.weather.io.abstract_async_weather_loader import AbstractAsyncWeatherLoader
from boiler.weather.io.abstract_sync_weather_reader import AbstractSyncWeatherReader
from boiler_softm.logger import logger


class SoftMAsyncWeatherForecastOnlineLoader(AbstractAsyncWeatherLoader):

    def __init__(self,
                 reader: AbstractSyncWeatherReader,
                 timestamp_filter_algorithm: AbstractTimestampFilterAlgorithm =
                 LeftClosedTimestampFilterAlgorithm(),
                 server_address: str = "https://lysva.agt.town",
                 http_proxy: Optional[str] = None,
                 sync_executor: ThreadPoolExecutor = None
                 ) -> None:
        self._weather_reader = reader
        self._weather_data_server_address = server_address
        self._timestamp_filter_algorithm = timestamp_filter_algorithm
        self._http_proxy = http_proxy
        self._sync_executor = sync_executor

        logger.debug(
            f"Creating instance: "
            f"reader: {self._weather_reader} "
            f"server_address: {self._weather_data_server_address} "
            f"timestamp_filter_algorithm: {self._timestamp_filter_algorithm} "
            f"http_proxy: {self._http_proxy} "
            f"sync_executor: {self._sync_executor} "
        )

    async def load_weather(self,
                           start_datetime: Optional[pd.Timestamp] = None,
                           end_datetime: Optional[pd.Timestamp] = None
                           ) -> pd.DataFrame:
        logger.debug(f"Requested weather forecast from {start_datetime} to {end_datetime}")
        raw_weather_forecast = await self._get_forecast_from_server()
        weather_df = await self._read_weather_forecast(raw_weather_forecast)
        weather_df = self._filter_by_timestamp(end_datetime, start_datetime, weather_df)
        logger.debug(f"Gathered {len(weather_df)} weather forecast items")
        return weather_df

    async def _get_forecast_from_server(self) -> bytes:
        url = f"{self._weather_data_server_address}/JSON"
        # noinspection SpellCheckingInspection
        params = {
            "method": "getPrognozT"
        }
        async with aiohttp.request("GET", url=url, params=params, proxy=self._http_proxy) as response:
            raw_response = await response.read()
            logger.debug(
                f"Weather forecast is loaded. "
                f"Response status code is {response.status}"
            )

        return raw_response

    async def _read_weather_forecast(self,
                                     raw_weather_forecast: bytes
                                     ) -> pd.DataFrame:
        loop = asyncio.get_running_loop()
        with io.BytesIO(raw_weather_forecast) as binary_stream:
            weather_forecast_df = await loop.run_in_executor(
                self._sync_executor,
                self._weather_reader.read_weather_from_binary_stream,
                binary_stream
            )
        return weather_forecast_df

    def _filter_by_timestamp(self,
                             end_datetime: Union[pd.Timestamp, None],
                             start_datetime: Union[pd.Timestamp, None],
                             weather_df: pd.DataFrame
                             ) -> pd.DataFrame:
        weather_df = self._timestamp_filter_algorithm.filter_df_by_min_max_timestamp(
            weather_df,
            start_datetime,
            end_datetime
        )
        return weather_df
