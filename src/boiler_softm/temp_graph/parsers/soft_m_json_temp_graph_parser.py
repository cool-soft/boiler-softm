import logging

import pandas as pd
from boiler.temp_graph.parsers.temp_graph_parser import TempGraphParser

from boiler_softm.constants import column_names_equal as softm_column_name_equals


class SoftMJSONTempGraphParser(TempGraphParser):

    def __init__(self) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance of the provider")

        self._column_names_equal = softm_column_name_equals.TEMP_GRAPH_COLUMN_NAMES_EQUALS

    # TODO: указать тип data
    def parse_temp_graph(self, data) -> pd.DataFrame:
        self._logger.debug("Parsing temp graph")

        df = pd.read_json(data)
        self._rename_columns(df)

        return df

    def _rename_columns(self, df: pd.DataFrame) -> None:
        self._logger.debug("Renaming column")

        df.rename(columns=self._column_names_equal, inplace=True)
