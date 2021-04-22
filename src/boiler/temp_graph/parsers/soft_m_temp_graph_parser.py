import logging

import pandas as pd

from ..constants import soft_m_column_names_equals
from .temp_graph_parser import TempGraphParser


class SoftMTempGraphParser(TempGraphParser):

    def __init__(self):
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance of the provider")

        self._column_names_equals = soft_m_column_names_equals.DICT

    def parse_temp_graph(self, temp_graph_as_text):
        self._logger.debug("Parsing temp graph")

        df = pd.read_json(temp_graph_as_text)
        self._rename_columns(df)

        return df

    def _rename_columns(self, df):
        self._logger.debug("Renaming column")

        df.rename(columns=self._column_names_equals, inplace=True)
