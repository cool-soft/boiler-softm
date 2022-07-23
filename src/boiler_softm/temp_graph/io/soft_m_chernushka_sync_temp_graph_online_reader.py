import json
from typing import BinaryIO

import pandas as pd
from boiler.temp_graph.io.abstract_sync_temp_graph_reader import AbstractSyncTempGraphReader

from boiler_softm.logging import logger
from boiler_softm.constants import converting_parameters


class SoftMChernushkaSyncTempGraphOnlineReader(AbstractSyncTempGraphReader):

    def __init__(self) -> None:
        self._column_names_equal = converting_parameters.CHERNUSHKA_TEMP_GRAPH_COLUMN_NAMES_EQUALS
        logger.debug("Creating instance")

    def read_temp_graph_from_binary_stream(self, binary_stream: BinaryIO) -> pd.DataFrame:
        logger.debug("Reading temp graph")
        temp_graph_server_response = json.load(binary_stream)
        df = pd.DataFrame(temp_graph_server_response.get("data"))
        df = df.rename(columns=self._column_names_equal)
        logger.debug("Temp graph is read")
        return df
