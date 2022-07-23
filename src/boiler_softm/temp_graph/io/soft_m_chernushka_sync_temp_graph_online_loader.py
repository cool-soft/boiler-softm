import json
from typing import Optional

import pandas as pd
import requests
from boiler.temp_graph.io.abstract_sync_temp_graph_loader import AbstractSyncTempGraphLoader
from boiler.temp_graph.io.abstract_sync_temp_graph_reader import AbstractSyncTempGraphReader

from boiler_softm.constants import api_constants
from boiler_softm.logging import logger


class SoftMChernushkaSyncTempGraphOnlineLoader(AbstractSyncTempGraphLoader):

    def __init__(self,
                 reader: AbstractSyncTempGraphReader,
                 boiler_id: int = 1,
                 api_base: str = api_constants.CHERNUSHKA_API_BASE,
                 http_proxy: Optional[str] = None,
                 https_proxy: Optional[str] = None
                 ) -> None:
        self._temp_graph_reader = reader
        self._boiler_id = boiler_id
        self._api_base = api_base
        self._proxies = {}
        if http_proxy is not None:
            self._proxies.update({"http": http_proxy})
        if https_proxy is not None:
            self._proxies.update({"https": https_proxy})

        logger.debug(
            f"Creating instance: "
            f"reader: {self._temp_graph_reader} "
            f"boiler_id: {self._boiler_id} "
            f"api_base: {self._api_base} "
            f"http_proxy: {http_proxy} "
        )

    def load_temp_graph(self) -> pd.DataFrame:
        logger.debug("Loading temp graph")
        url = f"{self._api_base}/JSON"
        params = {
            "method": "ai_getTempGraphic",
            "argument": json.dumps(
                {"boiler_id": self._boiler_id}
            )
        }
        with requests.get(url=url, params=params, proxies=self._proxies, stream=True) as response:
            logger.debug(f"Temp graph is loaded from server. Status code is {response.status_code}")
            temp_graph_df = self._temp_graph_reader.read_temp_graph_from_binary_stream(response.raw)
        return temp_graph_df
