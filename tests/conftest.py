import os

import pytest


@pytest.fixture(scope="session")
def is_need_proxy():
    need_proxy = False
    if os.getenv("TEST_WITH_PROXY") is not None:
        need_proxy = True
    return need_proxy


@pytest.fixture(scope="session")
def http_proxy_address():
    http_proxy_host = os.getenv("HTTP_PROXY_ADDRESS")
    http_proxy_port = os.getenv("HTTP_PROXY_PORT")
    return f"http://{http_proxy_host}:{http_proxy_port}"
