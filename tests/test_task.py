
from exaqute import ExaquteException
from exaqute import init, task, get_value_from_remote, barrier, delete_object

import pytest


@task()
def func1(x, y):
    return x + y


@task(returns=2)
def func2(x, y):
    return x + y, x * y


@task(returns=2)
def func3(x, y):
    return x * y


def test_task():
    init()
    assert 30 == get_value_from_remote(func1(10, 20, keep=True))

    v = func2(10, 20, keep=True)
    assert 30 == get_value_from_remote(v[0])
    assert 200 == get_value_from_remote(v[1])

    with pytest.raises(ExaquteException):
        func3(10, 20, keep=True)


def test_task_chaining():
    init()
    f = func1(10, 20)
    g = func1(f, 70, keep=True)
    assert get_value_from_remote(g) == 100

    with pytest.raises(ExaquteException):
        func1(f, 70)


def test_task_barrier_is_submit_point():
    init()
    f = func1(10, 20)

    barrier()

    with pytest.raises(ExaquteException):
        func1(f, 70)


def test_unkeep_task():
    init()
    f = func1(10, 20)
    with pytest.raises(ExaquteException):
        get_value_from_remote(f)


def test_delete_object():
    init()
    f = func1(10, 20, keep=True)
    delete_object(f)

    with pytest.raises(ExaquteException):
        delete_object(f)

    f = func1(10, 20)

    with pytest.raises(ExaquteException):
        delete_object(f)
