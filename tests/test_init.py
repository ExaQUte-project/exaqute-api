import pytest

from exaqute import ExaquteException, init, task


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
