# AGENTS.md

## Purpose

Guide for AI coding agents working on the PySpark FMCG training kit: exercise notebooks with `# TODO` cells and CHECK assertions, worked solutions, and a configurable synthetic dirty-data generator for a fictional FMCG retailer.

## Layout

- `notebooks/`: generated exercise notebooks; `notebooks/solutions/` holds the worked solutions. Do not hand-edit them; edit `scripts/exercises/` and regenerate.
- `scripts/`: `bootstrap_java.py` (portable Temurin JDK 17 into `.jdk/`), `build_notebooks.py` (renders notebooks from `scripts/exercises/`), `exercises/` (one module per notebook).
- `src/`: the `fmcg` data generator package (catalog, generator, dirty-data injection).
- `utils/`: `spark.py`, the SparkSession builder and dataset loaders (handles `JAVA_HOME`).
- Root files: `generate_data.py` (CLI entry point for data generation), `pyproject.toml`, `uv.lock`.

## Commands

- Install: `uv sync`, then `uv run python scripts/bootstrap_java.py` once (skip if `JAVA_HOME` points to JDK 17+).
- Lint: no linter is configured.
- Test: `uv run python generate_data.py`, then `uv run jupyter nbconvert --to notebook --execute --output-dir /tmp/fmcg-solutions notebooks/solutions/*.ipynb`; every CHECK cell must print ✅.
- Run: `uv run jupyter lab` and open `notebooks/00_environment_check.ipynb` first.
- Regenerate notebooks: `uv run python scripts/build_notebooks.py` after editing an exercise module.
- Deploy dry run: not applicable (nothing is deployed).

## Conventions

- Language: English for code, notebooks, docs and commit messages. Python 3.13 (`.python-version`).
- Public modules, classes and functions have docstrings. Comments explain why, not what. Tool directives such as # noqa are allowed.
- Formatters: none configured; follow the surrounding style.
- Exercises: prompts, solutions and CHECK cells come from the same `scripts/exercises/` module so they stay in sync. Spark 4 runs in ANSI mode; exercises use `try_cast` for dirty values.
- Commit style: Conventional Commits (`feat:`, `fix:`, `docs:`, `chore:`, `refactor:`, `test:`, `ci:`, optional scope such as `feat(exercises):`).
- PR branches: use `feat/`, `fix/`, `docs/`, or `chore/` followed by a short lowercase kebab-case description (for example, `fix/window-exercise-check`). Branch from current `origin/main` and open the PR against `main` unless the task specifies another base.
- PR titles: use `type(scope): short imperative summary` in English (for example, `fix(exercises): align solution check cell with prompt schema`). Choose the type for the change and the scope for the affected component, regardless of the branch prefix.
- PR body: follow the default template at [`RurociagDanych/.github` (`PULL_REQUEST_TEMPLATE.md`)](https://github.com/RurociagDanych/.github/blob/main/.github/PULL_REQUEST_TEMPLATE.md) (`Summary`, `Changes`, `Not changed and why`, `Owner decisions applied`, `Verification`, `Open risks`), filling every section and removing HTML comments; never merge PRs (the owner merges).
- Test locations: there is no `tests/` directory; the solution notebooks are the end-to-end checks.

## Guardrails

- Secrets: the kit needs no credentials; never commit any.
- Git: no force-push and no history rewrite on shared branches; never merge PRs (the owner merges).
- Deletes: do not delete files or notebooks without owner approval.
- Generated paths: `data/`, `.jdk/`, `.venv`, `spark-warehouse/`, `metastore_db/`, `derby.log` and `.ipynb_checkpoints/` stay untracked.
- Lock file: `uv.lock` is the authoritative Python lock; change dependencies in `pyproject.toml` and run `uv lock`.
