import os
from functools import wraps

from quake.job.globals import is_inside_job

from exaqute.common import ExaquteException

from .wrapper import WrapperConfig


def task(*, keep=False, returns=1, processes=1, **kwargs):
    def _builder(fn):
        wc = WrapperConfig(fn, keep, processes, returns, kwargs)
        fn.__quake_wrapper_config__ = wc

        @wraps(fn)
        def wrapper(*args, **kwargs):
            if is_inside_job():
                return fn(*args, **kwargs)
            else:
                return wc.make_task(*args, **kwargs)

        return wrapper

    return _builder


def mpi(*, runner, processes, **kwargs):
    def _builder(fn):
        if not hasattr(fn, "__wrapped__") and not hasattr(
            fn.__wrapped__, "__quake_wrapper_config__"
        ):
            raise Exception("mpi decorator used on non-task object")
        wc = fn.__quake_wrapper_config__
        wc._update_mpi_args(processes, kwargs)
        return fn

    return _builder


def constraint(computing_units=1):
    if isinstance(computing_units, str):
        try:
            # can be cast to int
            computing_units = int(computing_units)
        except Exception:
            if computing_units.startswith("$"):
                env_var = (
                    computing_units.replace("$", "").replace("{", "").replace("}", "")
                )
                try:
                    computing_units = int(os.environ[env_var])
                except Exception:
                    raise ExaquteException(
                        "Environment var: "
                        + env_var
                        + " not defined or can't be cast to int "
                    )
    assert isinstance(computing_units, int) and computing_units > 0
    return lambda x: x
