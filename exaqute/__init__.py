import os

from .common import *  # noqa

exaqute_backend = os.environ.get("EXAQUTE_BACKEND")

if exaqute_backend:
    print("EXAQUTE_BACKEND={}".format(exaqute_backend))
else:
    exaqute_backend = "local"
    print("Default ExaQUte backend: {}".format(exaqute_backend))

exaqute_backend = exaqute_backend.lower()

if exaqute_backend == "local":
    from .local import *  # noqa
elif exaqute_backend in ("quake", "pycompss"):
    raise ModuleNotFoundError(
        "ExaQUte backend known but not found: {}".format(exaqute_backend)
    )
else:
    raise ModuleNotFoundError("Unknown ExaQUte backend: {}".format(exaqute_backend))
