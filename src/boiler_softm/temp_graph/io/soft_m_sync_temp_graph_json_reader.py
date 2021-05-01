import io
import logging
from typing import BinaryIO

import pandas as pd
from boiler.temp_graph.io.sync.sync_temp_graph_reader import SyncTempGraphReader

from boiler_softm.constants import column_names_equal as softm_column_name_equals


class SoftMSyncTempGraphJSONReader(SyncTempGraphReader):

    def __init__(self,
                 encoding: str = "utf-8") -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        self._encoding = encoding
        self._column_names_equal = softm_column_name_equals.TEMP_GRAPH_COLUMN_NAMES_EQUALS

        self._logger.debug(f"Encoding is {encoding}")

    def set_encoding(self, encoding: str):
        self._logger.debug(f"Encoding is set to {encoding}")
        self._encoding = encoding

    def read_temp_graph_from_binary_stream(self, binary_stream: BinaryIO) -> pd.DataFrame:
        self._logger.debug("Reading temp graph")
        with io.TextIOWrapper(binary_stream, encoding=self._encoding) as text_stream:
            df = pd.read_json(text_stream)
        self._rename_columns(df)
        self._logger.debug("Temp graph is read")
        return df

    def _rename_columns(self, df: pd.DataFrame) -> None:
        self._logger.debug("Renaming columns")
        df.rename(columns=self._column_names_equal, inplace=True)
