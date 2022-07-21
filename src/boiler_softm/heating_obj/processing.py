from typing import Union, List

import pandas as pd
from boiler.constants import column_names
from boiler.data_processing.beetween_filter_algorithm \
    import AbstractTimestampFilterAlgorithm, LeftClosedTimestampFilterAlgorithm
from boiler.data_processing.timestamp_interpolator_algorithm import AbstractTimestampInterpolationAlgorithm
from boiler.data_processing.timestamp_round_algorithm import AbstractTimestampRoundAlgorithm
from boiler.data_processing.value_interpolation_algorithm import AbstractValueInterpolationAlgorithm
from boiler.heating_obj.processing import AbstractHeatingObjProcessor
from boiler_softm.logging import logger


class SoftMHeatingObjProcessor(AbstractHeatingObjProcessor):

    def __init__(self,
                 columns_to_interpolate: List[str],
                 timestamp_round_algorithm: AbstractTimestampRoundAlgorithm,
                 timestamp_interpolation_algorithm: AbstractTimestampInterpolationAlgorithm,
                 border_values_interpolation_algorithm: AbstractValueInterpolationAlgorithm,
                 internal_values_interpolation_algorithm: AbstractValueInterpolationAlgorithm,
                 timestamp_filter_algorithm: AbstractTimestampFilterAlgorithm =
                 LeftClosedTimestampFilterAlgorithm()
                 ) -> None:

        self._columns_to_process = columns_to_interpolate
        self._timestamp_round_algorithm = timestamp_round_algorithm
        self._timestamp_interpolation_algorithm = timestamp_interpolation_algorithm
        self._border_values_interpolation_algorithm = border_values_interpolation_algorithm
        self._internal_values_interpolation_algorithm = internal_values_interpolation_algorithm
        self._timestamp_filter_algorithm = timestamp_filter_algorithm

        logger.debug(
            f"Creating instance:"
            f"columns_to_interpolate: {self._columns_to_process}"
            f"timestamp_round_algorithm: {self._timestamp_round_algorithm}"
            f"timestamp_interpolation_algorithm: {self._timestamp_interpolation_algorithm}"
            f"border_values_interpolation_algorithm: {self._border_values_interpolation_algorithm}"
            f"internal_values_interpolation_algorithm: {self._internal_values_interpolation_algorithm}"
            f"timestamp_filter_algorithm: {self._timestamp_filter_algorithm}"
        )

    def process_heating_obj(self,
                            heating_obj_df: pd.DataFrame,
                            min_required_timestamp: Union[pd.Timestamp, None],
                            max_required_timestamp: Union[pd.Timestamp, None]
                            ) -> pd.DataFrame:
        logger.debug(f"Processing heating obj {min_required_timestamp}, {max_required_timestamp}")

        heating_obj_df = heating_obj_df.copy()
        heating_obj_df = self._round_timestamp(heating_obj_df)
        heating_obj_df = self._drop_duplicates_by_timestamp(heating_obj_df)
        heating_obj_df = self._interpolate_timestamp(heating_obj_df, max_required_timestamp, min_required_timestamp)
        heating_obj_df = self._interpolate_values(heating_obj_df)
        heating_obj_df = self._filter_by_timestamp(heating_obj_df, max_required_timestamp, min_required_timestamp)

        return heating_obj_df

    def _round_timestamp(self,
                         heating_obj_df: pd.DataFrame
                         ) -> pd.DataFrame:
        heating_obj_df = heating_obj_df.copy()
        heating_obj_df[column_names.TIMESTAMP] = self._timestamp_round_algorithm.round_series(
            heating_obj_df[column_names.TIMESTAMP]
        )
        return heating_obj_df

    # noinspection PyMethodMayBeStatic
    def _drop_duplicates_by_timestamp(self,
                                      heating_obj_df: pd.DataFrame
                                      ) -> pd.DataFrame:
        return heating_obj_df.drop_duplicates(column_names.TIMESTAMP, ignore_index=True, keep="last")

    def _interpolate_timestamp(self,
                               heating_obj_df: pd.DataFrame,
                               max_required_timestamp: Union[pd.Timestamp, None],
                               min_required_timestamp: Union[pd.Timestamp, None]
                               ) -> pd.DataFrame:
        heating_obj_df = self._timestamp_interpolation_algorithm.process_df(
            heating_obj_df,
            min_required_timestamp,
            max_required_timestamp
        )
        return heating_obj_df

    def _interpolate_values(self,
                            heating_obj_df: pd.DataFrame
                            ) -> pd.DataFrame:
        heating_obj_df = heating_obj_df.copy()
        for column_name in self._columns_to_process:
            heating_obj_df[column_name] = self._border_values_interpolation_algorithm.interpolate_series(
                heating_obj_df[column_name]
            )
            heating_obj_df[column_name] = self._internal_values_interpolation_algorithm.interpolate_series(
                heating_obj_df[column_name]
            )
        return heating_obj_df

    def _filter_by_timestamp(self,
                             heating_obj_df: pd.DataFrame,
                             max_required_timestamp: Union[pd.Timestamp, None],
                             min_required_timestamp: Union[pd.Timestamp, None]
                             ) -> pd.DataFrame:
        heating_obj_df = self._timestamp_filter_algorithm.filter_df_by_min_max_timestamp(
            heating_obj_df,
            min_required_timestamp,
            max_required_timestamp
        )
        return heating_obj_df
