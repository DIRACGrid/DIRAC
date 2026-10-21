# DIRAC Agent Guidelines

## Environment setup
- Unit tests and linters require an editable install, with DIRACCommon installed first (dependency resolution):
  ```bash
  pip install -e ./dirac-common[testing]
  pip install -e .[server,testing]
  ```

## Build/Lint/Test Commands
- **Unit tests**: `pytest` from the repo root. Configuration is in `pytest.ini`, which picks up `src/DIRAC/**/tests/Test_*.py` and ignores `tests/`.
- **Slow tests** are skipped by default; include them with `pytest --runslow`.
- **Single test**: `pytest src/DIRAC/<System>/tests/Test_foo.py::TestClass::test_method`
- **Lint**: `pre-commit run --all-files` (black, flynt, pyupgrade, bandit, …). CI also runs `pylint -j 0 -E src/` (errors only) and `ruff check src/`.
- **Type check**: `mypy` (configured in `pyproject.toml`).
- **Integration/System tests** (`tests/Integration`, `tests/System`): **not runnable standalone** — CI deploys them in containers against a full DIRAC server. Don't attempt to run or fix them locally.

## Repo layout
- One directory per system under `src/DIRAC/` (`WorkloadManagementSystem`, `DataManagementSystem`, …), plus cross-cutting `Core/`, `Resources/`, `Interfaces/`.
- Unit tests live beside the code, named `Test_*.py` (e.g. `src/DIRAC/Core/Utilities/tests/Test_*.py`).
- `tests/` at the repo root is integration/system/CI material only.
- **Changelog**: user-visible PRs add an entry to `release.notes` under the in-development version heading, grouped by `*<System>`, prefixed `NEW:`/`CHANGE:`/`FIX:`, with the PR number — see existing entries for the format.
- **Python compatibility**: code must stay Python 3.9+ compatible (pre-commit runs `pyupgrade --py39-plus`). Exception: `src/DIRAC/Resources/Computing/BatchSystems/*`, `PilotWrapper.py` and `tests/Integration/WorkloadManagementSystem/Test_GenerateAndExecutePilotWrapper.py` keep Python 2 compatibility (black py27 target) — don't reformat them by hand; pre-commit handles them.

## Code Style Guidelines
- **Formatting**: `black` (via pre-commit); line length 120 (configured in `pyproject.toml`)
- **Imports**: Absolute imports only; sort with `isort` (black profile)
- **Naming**: CamelCase for classes, snake_case for functions/variables
- **Types**: Use type hints; run `mypy` for strict checking
- **Error handling**: Return `S_OK(result)` or `S_ERROR(message)` from DIRAC.Core.Utilities.ReturnValues
- **Logging**: Use `gLogger.info/warn/error` (from DIRAC import gLogger)
- **Docstrings**: Follow Google/NumPy style where present
- **Security**: Never log secrets; validate inputs

## Agent skills

### Issue tracker

Issues are tracked in this repository's GitHub Issues (use the `gh` CLI). See `docs/agents/issue-tracker.md`.

### Domain docs

Single-context: `GLOSSARY.md` at the repo root + `docs/adr/`. See `docs/agents/domain.md`. Use the glossary's vocabulary (e.g. "Job" not "task", "Agent" only for server-side agents, "Pilot" for placeholder jobs) — see `GLOSSARY.md` for the _Avoid_ lists.
