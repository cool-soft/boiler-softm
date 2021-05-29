import os
import subprocess

import pytest


@pytest.fixture(scope="session")
def is_need_proxy():
    need_proxy = False
    if os.getenv("GITHUB_ACTIONS") is not None or \
            os.getenv("NEED_PROXY") is not None:
        need_proxy = True
    return need_proxy


@pytest.fixture(scope="session")
def ssh_proxy_address():
    return os.getenv("SSH_PROXY_ADDRESS")


@pytest.fixture(scope="session")
def http_proxy_address():
    return os.getenv("HTTP_PROXY_ADDRESS")


@pytest.fixture(scope="session")
def ssh_proxy_private_key_path():
    return os.getenv("SSH_PROXY_PRIVATE_KEY_PATH")


@pytest.fixture(scope="session")
def pproxy_python_path():
    return os.getenv("PPROXY_PYTHON_PATH")


@pytest.fixture(scope="session")
def ssh_proxy_user():
    return "root"


@pytest.fixture(scope="session", autouse=True)
def prepare_proxy(is_need_proxy,
                  pproxy_python_path,
                  http_proxy_address,
                  ssh_proxy_address,
                  ssh_proxy_user,
                  ssh_proxy_private_key_path,
                  request):
    if is_need_proxy:
        command = [
            f"{pproxy_python_path}",
            "-m", "pproxy",
            "-l", f"http://{http_proxy_address}",
            "-r", f"ssh://{ssh_proxy_address}#{ssh_proxy_user}::{ssh_proxy_private_key_path}",
        ]
        proxy_process = subprocess.Popen(command)
        request.addfinalizer(proxy_process.terminate)
