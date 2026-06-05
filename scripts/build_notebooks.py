"""Render the exercise definitions into paired exercise + solution notebooks.

For every notebook defined under ``scripts/exercises`` this writes:
  * ``notebooks/<slug>.ipynb``            — stubs + check cells (for the candidate)
  * ``notebooks/solutions/<slug>.ipynb``  — filled-in answers + the same checks

Usage::

    uv run python scripts/build_notebooks.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import nbformat as nbf
from nbformat.v4 import new_code_cell, new_markdown_cell, new_notebook

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

from exercises import NOTEBOOKS  # noqa: E402
from exercises.common import Notebook  # noqa: E402

CHECK_BANNER = "# ✅ CHECK — run this cell to grade your answer\n"

KERNEL_METADATA = {
    "kernelspec": {"name": "python3", "display_name": "Python 3", "language": "python"},
    "language_info": {"name": "python"},
}


def _render(notebook: Notebook, *, solution: bool) -> nbf.NotebookNode:
    cells = [
        new_markdown_cell(f"# {notebook.title}\n\n{notebook.intro}"),
        new_code_cell(notebook.setup),
    ]
    task_no = 0
    for task in notebook.tasks:
        if task.given:
            cells.append(new_markdown_cell(f"### {task.title}\n\n{task.prompt}"))
            cells.append(new_code_cell(task.solution))
            continue
        task_no += 1
        cells.append(new_markdown_cell(f"## Task {task_no}: {task.title}\n\n{task.prompt}"))
        cells.append(new_code_cell(task.solution if solution else task.stub))
        if task.check:
            cells.append(new_code_cell(CHECK_BANNER + task.check))

    closing = (
        "---\n🎉 **Solution notebook** — all cells should run top to bottom."
        if solution
        else "---\n🎉 Finished! Re-run the notebook top-to-bottom; every CHECK cell "
        "should print a ✅."
    )
    cells.append(new_markdown_cell(closing))

    nb = new_notebook()
    nb.cells = cells
    nb.metadata = KERNEL_METADATA
    return nb


def main() -> int:
    ex_dir = ROOT / "notebooks"
    sol_dir = ex_dir / "solutions"
    ex_dir.mkdir(parents=True, exist_ok=True)
    sol_dir.mkdir(parents=True, exist_ok=True)

    for notebook in NOTEBOOKS:
        ex_path = ex_dir / f"{notebook.slug}.ipynb"
        sol_path = sol_dir / f"{notebook.slug}.ipynb"
        nbf.write(_render(notebook, solution=False), ex_path)
        nbf.write(_render(notebook, solution=True), sol_path)
        print(f"  wrote {ex_path.relative_to(ROOT)} and {sol_path.relative_to(ROOT)}")

    print(f"\nDone: {len(NOTEBOOKS)} notebooks (exercise + solution).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
