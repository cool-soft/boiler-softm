import os

import pytest


@pytest.fixture(scope="session")
def is_need_proxy():
    need_proxy = False
    if os.getenv("TEST_WITH_PROXY") is not None:
        need_proxy = True
    return need_proxy


@pytest.fixture(scope="session")
def proxy_address():
    proxy_host = os.getenv("SOCKS_PROXY_ADDRESS")
    proxy_port = os.getenv("SOCKS_PROXY_PORT")
    return f"socks5://{proxy_host}:{proxy_port}"
