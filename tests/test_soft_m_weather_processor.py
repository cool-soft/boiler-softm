import pytest
import pandas as pd
from boiler.constants import column_names, dataset_prototypes
from dateutil.tz import gettz
from boiler.data_processing.beetween_filter_algorithm import FullClosedTimestampFilterAlgorithm
from boiler.data_processing.timestamp_round_algorithm import CeilTimestampRoundAlgorithm
from boiler.data_processing.timestamp_interpolator_algorithm import TimestampInterpolationAlgorithm
from boiler.data_processing.value_interpolation_algorithm import LinearInsideValueInterpolationAlgorithm
from boiler.data_processing.value_interpolation_algorithm import LinearOutsideValueInterpolationAlgorithm

from boiler_softm.constants.time_tick import TIME_TICK
from boiler_softm.weather.io.soft_m_sync_weather_forecast_online_loader import SoftMSyncWeatherForecastOnlineLoader
from boiler_softm.weather.io.soft_m_sync_weather_forecast_json_reader import SoftMSyncWeatherForecastJSONReader
from boiler_softm.weather.processing import SoftMWeatherProcessor


class TestSoftMWeatherProcessor:
    weather_data_timezone = gettz("Asia/Yekaterinburg")
    time_tick = TIME_TICK
    start_timestamp = pd.Timestamp.now(tz=weather_data_timezone) - (10 * time_tick)
    end_timestamp = start_timestamp + (20 * time_tick)

    @pytest.fixture
    def reader(self):
        return SoftMSyncWeatherForecastJSONReader(weather_data_timezone=gettz("Asia/Yekaterinburg"))

    @pytest.fixture
    def loader(self, reader, is_need_proxy, http_proxy_address):
        http_proxy = None
        https_proxy = None
        if is_need_proxy:
            http_proxy = http_proxy_address
            https_proxy = http_proxy_address
        loader = SoftMSyncWeatherForecastOnlineLoader(
            reader=reader,
            http_proxy=http_proxy,
            https_proxy=https_proxy
        )
        return loader

    @pytest.fixture
    def timestamp_round_algorithm(self):
        return CeilTimestampRoundAlgorithm(round_step=self.time_tick)

    @pytest.fixture
    def processor(self, timestamp_round_algorithm):
        return SoftMWeatherProcessor(
            timestamp_round_algorithm=timestamp_round_algorithm,
            timestamp_interpolation_algorithm=TimestampInterpolationAlgorithm(
                timestamp_round_algorithm,
                self.time_tick
            ),
            timestamp_filter_algorithm=FullClosedTimestampFilterAlgorithm(),
            border_values_interpolation_algorithm=LinearInsideValueInterpolationAlgorithm(),
            internal_values_interpolation_algorithm=LinearOutsideValueInterpolationAlgorithm()
        )

    def test_processor(self, loader, processor, timestamp_round_algorithm):
        weather_forecast = loader.load_weather(self.start_timestamp, self.end_timestamp)
        processed_forecast = processor.process_weather_df(
            weather_forecast,
            self.start_timestamp,
            self.end_timestamp
        )

        rounded_start_timestamp = timestamp_round_algorithm.round_value(self.start_timestamp)
        assert processed_forecast[column_names.TIMESTAMP].min() == rounded_start_timestamp
        rounded_end_timestamp = timestamp_round_algorithm.round_value(self.end_timestamp)
        assert processed_forecast[column_names.TIMESTAMP].max() == rounded_end_timestamp - self.time_tick

        for column_name in list(dataset_prototypes.WEATHER.columns):
            assert column_name in processed_forecast.columns

        for column_name in list(dataset_prototypes.WEATHER.columns):
            assert processed_forecast[column_name].isna().sum() == 0

        timestamp_list = processed_forecast[column_names.TIMESTAMP].to_list()
        for i in range(0, len(timestamp_list)-1):
            assert timestamp_list[i] + self.time_tick == timestamp_list[i+1]
