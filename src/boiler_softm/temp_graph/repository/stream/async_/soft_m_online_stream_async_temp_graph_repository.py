import logging

import aiohttp
import pandas as pd
from boiler.temp_graph.repository.stream.async_.temp_graph_stream_async_repository\
    import TempGraphStreamAsyncRepository
from boiler.temp_graph.parsers.temp_graph_parser import TempGraphParser


class SoftMOnlineStreamAsyncTempGraphRepository(TempGraphStreamAsyncRepository):

    def __init__(self,
                 server_address: str = "https://lysva.agt.town/",
                 temp_graph_parser: TempGraphParser = None) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance of the provider")

        self._temp_graph_server_address = server_address
        self._temp_graph_parser = temp_graph_parser

    def set_server_address(self, server_address: str) -> None:
        self._logger.debug(f"Server address is set to {server_address}")
        self._temp_graph_server_address = server_address

    def set_temp_graph_parser(self, temp_graph_parser: TempGraphParser) -> None:
        self._logger.debug("Temp graph parser is set")
        self._temp_graph_parser = temp_graph_parser

    async def get_temp_graph(self) -> pd.DataFrame:
        self._logger.debug(f"Requested temp graph")
        data = await self._get_temp_graph_from_server()
        temp_graph = self._temp_graph_parser.parse_temp_graph(data)
        return temp_graph

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

    async def set_temp_graph(self, temp_graph: pd.DataFrame) -> None:
        raise ValueError("Operation not supported for this type of repository")
