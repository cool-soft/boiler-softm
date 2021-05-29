import pytest
from boiler.constants import column_names
# noinspection PyProtectedMember
from pandas.api.types import is_numeric_dtype, is_datetime64tz_dtype
from dateutil.tz import gettz

from boiler_softm.weather.io.soft_m_async_weather_forecast_online_loader import \
    SoftMAsyncWeatherForecastOnlineLoader
from boiler_softm.weather.io.soft_m_sync_weather_forecast_json_reader import SoftMSyncWeatherForecastJSONReader


class TestSoftMAsyncWeatherForecastOnlineLoader:

    @pytest.fixture
    def reader(self):
        return SoftMSyncWeatherForecastJSONReader(weather_data_timezone=gettz("Asia/Yekaterinburg"))

    @pytest.fixture
    def loader(self, reader, is_need_proxy, http_proxy_address):
        if is_need_proxy:
            loader = SoftMAsyncWeatherForecastOnlineLoader(
                reader=reader,
                http_proxy=http_proxy_address
            )
        else:
            loader = SoftMAsyncWeatherForecastOnlineLoader(
                reader=reader
            )
        return loader

    @pytest.mark.asyncio
    async def test_soft_m_async_weather_forecast_online_loader(self, loader):
        weather_forecast_df = await loader.load_weather()

        assert not weather_forecast_df.empty

        assert column_names.TIMESTAMP in weather_forecast_df.columns
        assert column_names.WEATHER_TEMP in weather_forecast_df.columns

        assert is_datetime64tz_dtype(weather_forecast_df[column_names.TIMESTAMP])
        assert is_numeric_dtype(weather_forecast_df[column_names.WEATHER_TEMP])
