import asyncio
import io
from concurrent.futures.thread import ThreadPoolExecutor
from typing import Optional

import aiohttp
import pandas as pd
from boiler.temp_graph.io.abstract_async_temp_graph_loader import AbstractAsyncTempGraphLoader
from boiler.temp_graph.io.abstract_sync_temp_graph_reader import AbstractSyncTempGraphReader

from boiler_softm.constants.config_data_server import DATA_SERVER, GET_TEMP_GRAPH_METHOD
from boiler_softm.logger import logger


class SoftMAsyncTempGraphOnlineLoader(AbstractAsyncTempGraphLoader):

    def __init__(self,
                 reader: AbstractSyncTempGraphReader,
                 server_address: str = DATA_SERVER,
                 http_proxy: Optional[str] = None,
                 sync_executor: Optional[ThreadPoolExecutor] = None
                 ) -> None:
        self._temp_graph_reader = reader
        self._temp_graph_server_address = server_address
        self._http_proxy = http_proxy
        self._sync_executor = sync_executor

        logger.debug(
            f"Creating instance: "
            f"reader: {self._temp_graph_reader} "
            f"server_address: {self._temp_graph_server_address} "
            f"http_proxy: {self._http_proxy} "
            f"sync_executor: {self._sync_executor} "
        )

    async def load_temp_graph(self) -> pd.DataFrame:
        logger.debug("Loading temp graph")
        temp_graph_as_bytes = await self._get_temp_graph_from_server()
        temp_graph_df = await self._read_temp_graph(temp_graph_as_bytes)
        logger.debug(f"{temp_graph_df}")
        return temp_graph_df

    async def _get_temp_graph_from_server(self) -> bytes:
        url = f"{self._temp_graph_server_address}/JSON"
        params = {
            "method": GET_TEMP_GRAPH_METHOD,
            "argument": "{\"boiler_id\":1}"
        }
        async with aiohttp.request("GET", url=url, params=params, proxy=self._http_proxy) as response:
            raw_response = await response.read()
            logger.debug(f"Temp graph is loaded from server. "
                                      f"Response status code is {response.status}")

        raw_response = await self._get_data(raw_response)
        return raw_response

    async def _read_temp_graph(self, raw_temp_graph: bytes) -> pd.DataFrame:
        loop = asyncio.get_running_loop()
        with io.BytesIO(raw_temp_graph) as binary_stream:
            temp_graph_df = await loop.run_in_executor(
                self._sync_executor,
                self._temp_graph_reader.read_temp_graph_from_binary_stream,
                binary_stream
            )
        return temp_graph_df

    async def _get_data(self, raw_response: bytes):
        """
        Возвращает только данные, удаляя всю не нужную составляющую полученного ответа с сервера
        :param raw_response: ответ с сервера
        :return: только нужные данные
        """
        start = raw_response.find(b'[')
        end = raw_response.find(b']')
        return raw_response[start:end + 1]
