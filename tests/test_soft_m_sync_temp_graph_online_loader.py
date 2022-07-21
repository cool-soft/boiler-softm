import pytest
from boiler.constants import column_names
# noinspection PyProtectedMember
from pandas.api.types import is_numeric_dtype

from boiler_softm.temp_graph.io.soft_m_sync_temp_graph_online_loader import SoftMSyncTempGraphOnlineLoader
from boiler_softm.temp_graph.io.soft_m_sync_temp_graph_json_reader import SoftMSyncTempGraphJSONReader


class TestSoftMSyncTempGraphOnlineLoader:

    @pytest.fixture
    def reader(self):
        return SoftMSyncTempGraphJSONReader()

    @pytest.fixture
    def loader(self, reader, is_need_proxy, http_proxy_address):
        http_proxy = None
        if is_need_proxy:
            http_proxy = http_proxy_address
        loader = SoftMSyncTempGraphOnlineLoader(
            reader=reader,
            http_proxy=http_proxy
        )
        return loader

    def test_soft_m_sync_temp_graph_online_loader(self, loader):
        temp_graph_df = loader.load_temp_graph()

        assert not temp_graph_df.empty

        assert column_names.WEATHER_TEMP in temp_graph_df.columns
        assert column_names.FORWARD_PIPE_COOLANT_TEMP in temp_graph_df.columns
        assert column_names.BACKWARD_PIPE_COOLANT_TEMP in temp_graph_df.columns

        assert is_numeric_dtype(temp_graph_df[column_names.WEATHER_TEMP])
        assert is_numeric_dtype(temp_graph_df[column_names.FORWARD_PIPE_COOLANT_TEMP])
        assert is_numeric_dtype(temp_graph_df[column_names.BACKWARD_PIPE_COOLANT_TEMP])
