import logging
from datetime import timedelta

import pandas as pd

from ...constants import column_names
from .heating_system_data_interpolator import HeatingSystemDataInterpolator


class HeatingSystemDataLinearInterpolator(HeatingSystemDataInterpolator):

    def __init__(self):
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance of the provider")

        self._columns_to_interpolate = (
            column_names.FORWARD_PIPE_COOLANT_TEMP,
            column_names.BACKWARD_PIPE_COOLANT_TEMP,
            column_names.FORWARD_PIPE_COOLANT_VOLUME,
            column_names.BACKWARD_PIPE_COOLANT_VOLUME,
            column_names.FORWARD_PIPE_COOLANT_PRESSURE,
            column_names.BACKWARD_PIPE_COOLANT_PRESSURE
        )
        self._interpolation_step = timedelta(seconds=180)

    def set_columns_to_interpolate(self, columns):
        self._columns_to_interpolate = columns

    def set_interpolation_step(self, interpolation_step):
        self._interpolation_step = interpolation_step

    def interpolate_data(
            self,
            df: pd.DataFrame,
            start_datetime=None,
            end_datetime=None,
            inplace=False
    ) -> pd.DataFrame:
        self._logger.debug("Interpolating is requested")

        if not inplace:
            df = df.copy()

        self._round_datetime(df)

        df.sort_values(by=column_names.TIMESTAMP, ignore_index=True, inplace=True)
        df = self._interpolate_border_datetime(df, start_datetime, end_datetime)
        df.sort_values(by=column_names.TIMESTAMP, ignore_index=True, inplace=True)
        df = self._interpolate_passes_of_datetime(df)

        self._interpolate_border_data(df)
        self._interpolate_passes_of_data(df)

        return df

    def _round_datetime(self, df):
        self._logger.debug("Rounding datetime")

        interpolations_step_in_seconds = int(self._interpolation_step.total_seconds())
        df[column_names.TIMESTAMP] = df[column_names.TIMESTAMP].dt.round(f"{interpolations_step_in_seconds}s")
        df.drop_duplicates(column_names.TIMESTAMP, inplace=True, ignore_index=True)

    # noinspection PyMethodMayBeStatic
    def _interpolate_border_datetime(self,
                                     df: pd.DataFrame,
                                     start_datetime: pd.Timestamp,
                                     end_datetime: pd.Timestamp):
        self._logger.debug("Interpolating border datetime values")

        if start_datetime is not None:
            start_datetime = start_datetime.floor(f"{int(self._interpolation_step.total_seconds())}s")
            first_datetime_idx = df[column_names.TIMESTAMP].idxmin()
            first_row = df.loc[first_datetime_idx]
            first_datetime = first_row[column_names.TIMESTAMP]
            if first_datetime > start_datetime:
                df = df.append({column_names.TIMESTAMP: start_datetime}, ignore_index=True)

        if end_datetime is not None:
            end_datetime = end_datetime.floor(f"{int(self._interpolation_step.total_seconds())}s")
            last_datetime_idx = df[column_names.TIMESTAMP].idxmax()
            last_row = df.loc[last_datetime_idx]
            last_datetime = last_row[column_names.TIMESTAMP]
            if last_datetime < end_datetime:
                df = df.append({column_names.TIMESTAMP: end_datetime}, ignore_index=True)

        return df

    def _interpolate_passes_of_datetime(self, df: pd.DataFrame):
        self._logger.debug("Interpolating passes of datetime")

        datetime_to_insert = []
        previous_datetime = None
        for timestamp in df[column_names.TIMESTAMP].to_list():
            if previous_datetime is None:
                previous_datetime = timestamp
                continue
            next_datetime = timestamp

            current_datetime = previous_datetime + self._interpolation_step
            while current_datetime < next_datetime:
                datetime_to_insert.append({
                    column_names.TIMESTAMP: current_datetime
                })
                current_datetime += self._interpolation_step

            previous_datetime = next_datetime

        df = df.append(datetime_to_insert, ignore_index=True)
        df.sort_values(by=column_names.TIMESTAMP, ignore_index=True, inplace=True)

        return df

    def _interpolate_border_data(self, df):
        self._logger.debug("Interpolating border data values")

        first_datetime_index = df[column_names.TIMESTAMP].idxmin()
        last_datetime_index = df[column_names.TIMESTAMP].idxmax()

        for column_name in self._columns_to_interpolate:
            first_valid_index = df[column_name].first_valid_index()
            if first_valid_index != first_datetime_index:
                first_valid_value = df.loc[first_valid_index, column_name]
                df.loc[first_datetime_index, column_name] = first_valid_value

            last_valid_index = df[column_name].last_valid_index()
            if last_valid_index != last_datetime_index:
                last_valid_value = df.loc[last_valid_index, column_name]
                df.loc[last_datetime_index, column_name] = last_valid_value

    def _interpolate_passes_of_data(self, df):
        self._logger.debug("Interpolating passes of data")
        for column_to_interpolate in self._columns_to_interpolate:
            df[column_to_interpolate] = pd.to_numeric(df[column_to_interpolate], downcast="float")
            df[column_to_interpolate].interpolate(inplace=True)
