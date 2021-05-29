import pytest
from boiler.constants import column_names
# noinspection PyProtectedMember
from pandas.api.types import is_numeric_dtype

from boiler_softm.temp_graph.io.soft_m_async_temp_graph_online_loader import SoftMAsyncTempGraphOnlineLoader
from boiler_softm.temp_graph.io.soft_m_sync_temp_graph_json_reader import SoftMSyncTempGraphJSONReader


class TestSoftMAsyncTempGraphOnlineLoader:

    @pytest.fixture
    def reader(self):
        return SoftMSyncTempGraphJSONReader()

    @pytest.fixture
    def loader(self, reader, is_need_proxy, http_proxy_address):
        if is_need_proxy:
            loader = SoftMAsyncTempGraphOnlineLoader(
                reader=reader,
                http_proxy=http_proxy_address
            )
        else:
            loader = SoftMAsyncTempGraphOnlineLoader(
                reader=reader
            )
        return loader

    @pytest.mark.asyncio
    async def test_soft_m_async_temp_graph_online_loader(self, loader):
        temp_graph_df = await loader.load_temp_graph()

        assert not temp_graph_df.empty

        assert column_names.WEATHER_TEMP in temp_graph_df.columns
        assert column_names.FORWARD_PIPE_COOLANT_TEMP in temp_graph_df.columns
        assert column_names.BACKWARD_PIPE_COOLANT_TEMP in temp_graph_df.columns

        assert is_numeric_dtype(temp_graph_df[column_names.WEATHER_TEMP])
        assert is_numeric_dtype(temp_graph_df[column_names.FORWARD_PIPE_COOLANT_TEMP])
        assert is_numeric_dtype(temp_graph_df[column_names.BACKWARD_PIPE_COOLANT_TEMP])
