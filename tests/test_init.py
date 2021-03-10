
from exaqute import ExaquteException
from exaqute import init, task


import pytest


def test_double_init():
    init()
    with pytest.raises(ExaquteException):
        init()
    with pytest.raises(ExaquteException):
        init()


def test_no_init():

    @task()
    def f():
        pass

    with pytest.raises(ExaquteException):
        f()
