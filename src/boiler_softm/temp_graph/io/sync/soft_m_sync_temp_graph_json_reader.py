import logging
from typing import TextIO

import pandas as pd
from boiler.temp_graph.io.sync.sync_temp_graph_text_reader import SyncTempGraphTextReader

from boiler_softm.constants import column_names_equal as softm_column_name_equals


class SoftMSyncTempGraphJSONReader(SyncTempGraphTextReader):

    def __init__(self) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        self._column_names_equal = softm_column_name_equals.TEMP_GRAPH_COLUMN_NAMES_EQUALS

    # TODO: указать тип data
    def read_temp_graph_from_text_io(self, text_io: TextIO) -> pd.DataFrame:
        self._logger.debug("Reading temp graph")
        df = pd.read_json(text_io)
        self._rename_columns(df)
        self._logger.debug("Temp graph is read")
        return df

    def _rename_columns(self, df: pd.DataFrame) -> None:
        self._logger.debug("Renaming columns")
        df.rename(columns=self._column_names_equal, inplace=True)
