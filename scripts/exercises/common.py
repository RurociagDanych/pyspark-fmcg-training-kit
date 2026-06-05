"""Shared building blocks for defining training notebooks.

A notebook is a title + intro + a shared setup cell + a list of tasks. Each task
carries the markdown prompt, the stub the candidate sees, the reference
solution, and an auto-check cell that asserts on the candidate's result. The
build script renders an *exercise* notebook (stubs) and a *solution* notebook
(filled in) from the same definition so they never drift apart.
"""

from __future__ import annotations

from dataclasses import dataclass, field

# Shared setup prepended to every notebook. Finds the repo root from the
# notebook's location so imports work whether run from notebooks/ or
# notebooks/solutions/.
SETUP_HEADER = '''import sys
from pathlib import Path

_root = Path.cwd()
while not (_root / "utils" / "spark.py").exists() and _root != _root.parent:
    _root = _root.parent
for _p in (str(_root), str(_root / "src")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from utils.spark import build_session, read_dim, read_sales_raw, read_sales_typed

spark = build_session("{app}")
print("Spark", spark.version, "ready")'''


@dataclass
class Task:
    """One exercise within a notebook.

    Args:
        title: short task title.
        prompt: markdown describing what to do.
        solution: reference implementation (shown in the solution notebook).
        stub: starter code shown in the exercise notebook.
        check: assertion cell appended after the answer (graded both ways).
        given: if True, this is provided/demo code, not a task — both notebooks
            show ``solution``, no stub or check, and no "Task N" numbering.
    """

    title: str
    prompt: str
    solution: str
    stub: str = ""
    check: str = ""
    given: bool = False


@dataclass
class Notebook:
    slug: str       # filename stem, e.g. "01_core_dataframe_ops"
    title: str
    intro: str
    setup: str
    tasks: list[Task] = field(default_factory=list)


def setup_cell(app: str, extra: str = "") -> str:
    """Render the shared setup cell, optionally with extra data-loading code."""
    body = SETUP_HEADER.format(app=app)
    return f"{body}\n\n{extra}" if extra else body
