import asyncio
import io
import logging
from typing import Optional

import aiohttp
import pandas as pd
from boiler.temp_graph.io.async_.async_temp_graph_loader import AsyncTempGraphLoader
from boiler.temp_graph.io.sync.sync_temp_graph_text_reader import SyncTempGraphTextReader


class SoftMAsyncTempGraphOnlineLoader(AsyncTempGraphLoader):

    def __init__(self,
                 server_address: str = "https://lysva.agt.town/",
                 reader: Optional[SyncTempGraphTextReader] = None) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance of the provider")

        self._temp_graph_server_address = server_address
        self._temp_graph_reader = reader

    def set_server_address(self, server_address: str) -> None:
        self._logger.debug(f"Server address is set to {server_address}")
        self._temp_graph_server_address = server_address

    def set_temp_graph_reader(self, temp_graph_parser: SyncTempGraphTextReader) -> None:
        self._logger.debug("Temp graph parser is set")
        self._temp_graph_reader = temp_graph_parser

    async def load_temp_graph(self) -> pd.DataFrame:
        self._logger.debug(f"Requested temp graph")
        temp_graph_as_str = await self._get_temp_graph_from_server()
        temp_graph_df = await self._read_temp_graph(temp_graph_as_str)
        return temp_graph_df

    async def _get_temp_graph_from_server(self) -> str:
        self._logger.debug(f"Requesting temp graph from server {self._temp_graph_server_address}")

        url = f"{self._temp_graph_server_address}/JSON/"
        params = {
            "method": "getTempGraphic"
        }
        async with aiohttp.request("GET", url=url, params=params) as response:
            response_text = await response.text()
            self._logger.debug(f"Temp graph is loaded from server. "
                               f"Response status code is {response.status}")
        return response_text

    async def _read_temp_graph(self, temp_graph_as_str: str) -> pd.DataFrame:
        self._logger.debug("Reading temp graph in executor")
        temp_graph_as_text_io = io.StringIO(temp_graph_as_str)
        loop = asyncio.get_running_loop()
        temp_graph_df = await loop.run_in_executor(
            None,
            self._temp_graph_reader.read_temp_graph_from_text_io,
            temp_graph_as_text_io
        )
        self._logger.debug("Temp graph is read")
        return temp_graph_df
