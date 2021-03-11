import os.path
import sys

import pytest

TEST_DIR = os.path.dirname(__file__)
ROOT = os.path.dirname(TEST_DIR)

sys.path.insert(0, ROOT)

import exaqute.local as exaqute_local  # noqa


@pytest.fixture(autouse=True)
def env_reset_init():
    yield
    exaqute_local.internals._reset()
