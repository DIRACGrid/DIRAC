# DIRAC Agent Guidelines

## Build/Lint/Test Commands
- **Build**: `pip install -e .`
- **Lint**: `ruff check src/ && pylint src/`
- **Test**: `pytest tests/`
- **Single test**: `pytest src/DIRAC/path/to/test.py::test_function`

## Code Style Guidelines
- **Formatting**: Use `black` with line length 120 (configured in pyproject.toml)
- **Imports**: Absolute imports only; sort with `isort` (black profile)
- **Naming**: CamelCase for classes, snake_case for functions/variables
- **Types**: Use type hints; run `mypy` for strict checking
- **Error handling**: Return `S_OK(result)` or `S_ERROR(message)` from DIRAC.Core.Utilities.ReturnValues
- **Logging**: Use `gLogger.info/warn/error` (from DIRAC import gLogger)
- **Docstrings**: Follow Google/NumPy style where present
- **Security**: Never log secrets; validate inputs
