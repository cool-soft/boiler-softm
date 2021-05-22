import asyncio
import io
import logging
from concurrent.futures.thread import ThreadPoolExecutor
from typing import Optional

import aiohttp
import pandas as pd
from boiler.temp_graph.io.abstract_async_temp_graph_loader import AbstractAsyncTempGraphLoader
from boiler.temp_graph.io.abstract_sync_temp_graph_reader import AbstractSyncTempGraphReader


class SoftMAsyncTempGraphOnlineLoader(AbstractAsyncTempGraphLoader):

    def __init__(self,
                 reader: AbstractSyncTempGraphReader,
                 server_address: str = "https://lysva.agt.town/",
                 sync_executor: Optional[ThreadPoolExecutor] = None
                 ) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)

        self._temp_graph_reader = reader
        self._temp_graph_server_address = server_address
        self._sync_executor = sync_executor

    async def load_temp_graph(self) -> pd.DataFrame:
        self._logger.debug(f"Requested temp graph")
        temp_graph_as_bytes = await self._get_temp_graph_from_server()
        temp_graph_df = await self._read_temp_graph(temp_graph_as_bytes)
        return temp_graph_df

    async def _get_temp_graph_from_server(self) -> bytes:
        url = f"{self._temp_graph_server_address}/JSON/"
        params = {
            "method": "getTempGraphic"
        }
        async with aiohttp.request("GET", url=url, params=params) as response:
            raw_response = await response.read()
            self._logger.debug(f"Temp graph is loaded from server. "
                               f"Response status code is {response.status}")

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