
from exaqute import task, constraint

import pytest


def test_constraint():

    @constraint(computing_units=2)
    @task()
    def _(x, y):
        return x + y

    with pytest.raises(AssertionError):
        @constraint(computing_units="abc")
        @task()
        def _2(x, y):
            return x + y
