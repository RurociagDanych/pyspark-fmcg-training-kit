"""Exercise content for the FMCG training notebooks.

Each ``nbXX_*`` module exposes ``get() -> Notebook``. ``scripts/build_notebooks``
renders these into paired exercise + solution ``.ipynb`` files.
"""

from . import nb00_environment, nb01_core, nb02_joins_windows, nb03_data_quality, nb04_performance

NOTEBOOKS = [
    nb00_environment.get(),
    nb01_core.get(),
    nb02_joins_windows.get(),
    nb03_data_quality.get(),
    nb04_performance.get(),
]
