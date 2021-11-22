import os

from .common import *  # noqa

exaqute_backend = os.environ.get("EXAQUTE_BACKEND", "local")
print("EXAQUTE_BACKEND =", exaqute_backend)

exaqute_backend = exaqute_backend.lower()

if exaqute_backend == "local":
    from .local import *  # noqa
elif exaqute_backend == "quake":
    from .quake import *  # noqa
elif exaqute_backend == "hyperloom":
    from .loom import *  # noqa
elif exaqute_backend == "pycompss":
    from .pycompss import *  # noqa
else:
    raise ModuleNotFoundError(
        (
            "Unknown ExaQUte backend: {}\n"
            "Supported values are 'local', 'hyperloom', 'quake' and 'pycompss'"
        ).format(exaqute_backend)
    )
