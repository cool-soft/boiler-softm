import os

import pytest


@pytest.fixture(scope="session")
def is_need_proxy():
    need_proxy = False
    if os.getenv("TEST_WITH_PROXY") is not None:
        need_proxy = True
    return need_proxy


@pytest.fixture(scope="session")
def ssh_proxy_address():
    return os.getenv("SSH_PROXY_ADDRESS")


@pytest.fixture(scope="session")
def http_proxy_address():
    http_proxy_host = os.getenv("HTTP_PROXY_HOSTNAME")
    http_proxy_port = os.getenv("HTTP_PROXY_PORT")
    return f"{http_proxy_host}:{http_proxy_port}"


@pytest.fixture(scope="session")
def ssh_proxy_private_key_path():
    return os.getenv("SSH_PROXY_PRIVATE_KEY_PATH")
