import logging

from boiler.constants import column_names


class CorrTableTempPredictor:

    def __init__(self,
                 temp_correlation_table=None,
                 home_time_deltas=None,
                 home_min_temp_coefficient=1):
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance of the provider")

        self._temp_correlation_table = temp_correlation_table
        self._homes_time_deltas = home_time_deltas
        self._home_min_temp_coefficient = home_min_temp_coefficient

        self._logger.debug(f"Home min temp coefficient is {home_min_temp_coefficient}")

    def set_homes_time_deltas(self, homes_time_deltas):
        self._logger.debug("Set homes time deltas")
        self._homes_time_deltas = homes_time_deltas

    def get_homes_time_deltas(self):
        return self._homes_time_deltas.copy()

    def set_temp_correlation_table(self, temp_correlation_table):
        self._logger.debug("Set temp correlation table")
        self._temp_correlation_table = temp_correlation_table

    def set_home_min_temp_coefficient(self, min_temp_coefficient):
        logging.debug(f"Set home min temp coefficient to {min_temp_coefficient}")
        self._home_min_temp_coefficient = min_temp_coefficient

    def predict_on_temp_requirements(self, temp_requirements):
        max_home_time_delta = self._homes_time_deltas[column_names.TIME_DELTA].max()
        boiler_temp_count = len(temp_requirements) - max_home_time_delta

        boiler_temp_list = []
        for time_moment_number in range(boiler_temp_count):
            need_boiler_temp = self._calc_boiler_temp_for_time_moment(time_moment_number, temp_requirements)
            boiler_temp_list.append(need_boiler_temp)

        return boiler_temp_list

    def _calc_boiler_temp_for_time_moment(self, time_moment_number, temp_requirements_arr):
        need_boiler_temp = float("-inf")

        home_names_list = self._homes_time_deltas[column_names.HOME_NAME].to_list()
        time_deltas_list = self._homes_time_deltas[column_names.TIME_DELTA].to_list()
        for home_name, home_time_delta in zip(home_names_list, time_deltas_list):
            need_home_temp = temp_requirements_arr[time_moment_number + home_time_delta]
            need_home_temp *= self._home_min_temp_coefficient
            need_temp_condition = self._temp_correlation_table[home_name] >= need_home_temp
            need_boiler_temp_for_home = self._temp_correlation_table[
                need_temp_condition][column_names.CORRELATED_BOILER_TEMP].min()
            need_boiler_temp = max(need_boiler_temp, need_boiler_temp_for_home)

        return need_boiler_temp
