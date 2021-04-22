import logging

import pandas as pd

from boiler.constants import column_names
from boiler.temp_requirements.calculators.utils import arithmetic_round


class TempGraphRequirementsCalculator:

    def __init__(self, temp_graph: pd.DataFrame = None):
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance of the provider")

        self._temp_graph = temp_graph

    def set_temp_graph(self, temp_graph):
        self._logger.debug("Temp graph is set")
        self._temp_graph = temp_graph

    def get_temp_requirements_for_weather_temp(self, weather_temp):
        weather_temp = arithmetic_round(weather_temp)
        available_temp = self._temp_graph[self._temp_graph[column_names.WEATHER_TEMP] <= weather_temp]
        if not available_temp.empty:
            required_temp_idx = available_temp[column_names.WEATHER_TEMP].idxmax()
        else:
            required_temp_idx = self._temp_graph[column_names.WEATHER_TEMP].idxmin()
            self._logger.debug(f"Weather temp {weather_temp} is not in temp graph.")
        required_temp = self._temp_graph.loc[required_temp_idx].copy()

        return required_temp
