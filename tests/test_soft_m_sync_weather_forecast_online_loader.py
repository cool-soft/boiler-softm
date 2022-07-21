import pytest
from boiler.constants import column_names
# noinspection PyProtectedMember
from pandas.api.types import is_numeric_dtype, is_datetime64tz_dtype
from dateutil.tz import gettz

from boiler_softm.weather.io.soft_m_sync_weather_forecast_online_loader import \
    SoftMSyncWeatherForecastOnlineLoader
from boiler_softm.weather.io.soft_m_sync_weather_forecast_json_reader import SoftMSyncWeatherForecastJSONReader


class TestSoftMSyncWeatherForecastOnlineLoader:

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

    def test_soft_m_sync_weather_forecast_online_loader(self, loader):
        weather_forecast_df = loader.load_weather()

        assert not weather_forecast_df.empty

        assert column_names.TIMESTAMP in weather_forecast_df.columns
        assert column_names.WEATHER_TEMP in weather_forecast_df.columns

        assert is_datetime64tz_dtype(weather_forecast_df[column_names.TIMESTAMP])
        assert is_numeric_dtype(weather_forecast_df[column_names.WEATHER_TEMP])
