import asyncio
import io
import logging
from typing import Optional

import aiohttp
import pandas as pd
from boiler.temp_graph.io.async_.async_temp_graph_loader import AsyncTempGraphLoader
from boiler.temp_graph.io.sync.sync_temp_graph_reader import SyncTempGraphReader


class SoftMAsyncTempGraphOnlineLoader(AsyncTempGraphLoader):

    def __init__(self,
                 server_address: str = "https://lysva.agt.town/",
                 reader: Optional[SyncTempGraphReader] = None) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance of the provider")

        self._temp_graph_server_address = server_address
        self._temp_graph_reader = reader

    def set_server_address(self, server_address: str) -> None:
        self._logger.debug(f"Server address is set to {server_address}")
        self._temp_graph_server_address = server_address

    def set_temp_graph_reader(self, temp_graph_parser: SyncTempGraphReader) -> None:
        self._logger.debug("Temp graph parser is set")
        self._temp_graph_reader = temp_graph_parser

    async def load_temp_graph(self) -> pd.DataFrame:
        self._logger.debug(f"Requested temp graph")
        temp_graph_as_bytes = await self._get_temp_graph_from_server()
        temp_graph_df = await self._read_temp_graph(temp_graph_as_bytes)
        return temp_graph_df

    async def _get_temp_graph_from_server(self) -> bytes:
        self._logger.debug(f"Requesting temp graph from server {self._temp_graph_server_address}")

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
        self._logger.debug("Reading temp graph in executor")

        loop = asyncio.get_running_loop()
        with io.BytesIO(raw_temp_graph) as binary_stream:
            temp_graph_df = await loop.run_in_executor(
                None,
                self._temp_graph_reader.read_temp_graph_from_binary_stream,
                binary_stream
            )
        self._logger.debug("Temp graph is read")

        return temp_graph_df
