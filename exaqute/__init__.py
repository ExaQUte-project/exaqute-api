import os

from .common import *  # noqa

exacute_backend = os.environ.get("EXAQUTE_BACKEND", "local")
print("EXAQUTE_BACKEND =", exacute_backend)

exacute_backend = exacute_backend.lower()

if exacute_backend == "local":
    from .local import *  # noqa
elif exacute_backend == "quake":
    from .quake import *  # noqa
elif exacute_backend == "pycompss":
    from .pycompss import *  # noqa
else:
    raise Exception("Unknown exaqute backend: {}".format(exacute_backend))
