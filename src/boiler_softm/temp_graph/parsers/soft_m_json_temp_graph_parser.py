import logging

import pandas as pd
from boiler.temp_graph.parsers.temp_graph_parser import TempGraphParser

from boiler_softm.constants import column_names_equals as softm_column_names_equals


class SoftMJSONTempGraphParser(TempGraphParser):

    def __init__(self):
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance of the provider")

        self._column_names_equals = softm_column_names_equals.TEMP_GRAPH_COLUMN_NAMES_EQUALS

    def parse_temp_graph(self, temp_graph_as_text):
        self._logger.debug("Parsing temp graph")

        df = pd.read_json(temp_graph_as_text)
        self._rename_columns(df)

        return df

    def _rename_columns(self, df):
        self._logger.debug("Renaming column")

        df.rename(columns=self._column_names_equals, inplace=True)
