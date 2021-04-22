import logging

import numpy as np
import pandas as pd

from boiler.time_delta.time_delta_calculator import TimeDeltaCalculator


class StdVarTimeDeltaCalculator(TimeDeltaCalculator):

    def __init__(self,
                 x_round_step: float = 0.1,
                 min_lag: int = 1,
                 max_lag: int = 20):
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        self._x_round_step = x_round_step
        self._min_lag = min_lag
        self._max_lag = max_lag

    def set_x_round_step(self, round_step: float):
        self._logger.debug(f"Round step for x is set to {round_step}")
        self._x_round_step = round_step

    def set_min_lag(self, lag: int):
        self._logger.debug(f"Min lag is set to {lag}")
        self._min_lag = lag

    def set_max_lag(self, lag: int):
        self._logger.debug(f"Max lag is set to {lag}")
        self._max_lag = lag

    def find_lag(self, x: np.ndarray, y: np.ndarray) -> int:
        self._logger.debug("Lag calculation is requested")

        rounded_x_column = "rounded_x"
        y_column = "y"

        min_std = float("inf")
        lag = None
        for test_lag in range(self._min_lag, self._max_lag):
            x_with_lag = x[:-test_lag]
            y_with_lag = y[test_lag:]

            rounded_x = x_with_lag // self._x_round_step * self._x_round_step

            correlation_df = pd.DataFrame({
                rounded_x_column: rounded_x,
                y_column: y_with_lag
            })
            mean_series = correlation_df.groupby(rounded_x_column)[y_column].mean()
            y_mean_group_value = (correlation_df[rounded_x_column].replace(mean_series)).to_numpy()
            y_delta = y_with_lag - y_mean_group_value

            # noinspection PyUnresolvedReferences
            std_var = np.std(y_delta)

            if std_var < min_std:
                min_std = std_var
                lag = test_lag

        return lag
