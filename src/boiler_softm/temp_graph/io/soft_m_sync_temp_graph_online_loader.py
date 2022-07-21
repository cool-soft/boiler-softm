from typing import Optional

import pandas as pd
import requests
from boiler.temp_graph.io.abstract_sync_temp_graph_loader import AbstractSyncTempGraphLoader
from boiler.temp_graph.io.abstract_sync_temp_graph_reader import AbstractSyncTempGraphReader

from boiler_softm.logging import logger


class SoftMSyncTempGraphOnlineLoader(AbstractSyncTempGraphLoader):

    def __init__(self,
                 reader: AbstractSyncTempGraphReader,
                 server_address: str = "https://lysva.agt.town",
                 http_proxy: Optional[str] = None
                 ) -> None:
        self._temp_graph_reader = reader
        self._temp_graph_server_address = server_address
        self._proxies = None
        if http_proxy is not None:
            self._proxies = {"https": http_proxy}

        logger.debug(
            f"Creating instance: "
            f"reader: {self._temp_graph_reader} "
            f"server_address: {self._temp_graph_server_address} "
            f"http_proxy: {http_proxy} "
        )

    def load_temp_graph(self) -> pd.DataFrame:
        logger.debug("Loading temp graph")
        url = f"{self._temp_graph_server_address}/JSON"
        params = {
            "method": "getTempGraphic"
        }
        with requests.get(url=url, params=params, proxies=self._proxies, stream=True) as response:
            logger.debug(f"Temp graph is loaded from server. Status code is {response.status_code}")
            temp_graph_df = self._temp_graph_reader.read_temp_graph_from_binary_stream(response.raw)
        return temp_graph_df
