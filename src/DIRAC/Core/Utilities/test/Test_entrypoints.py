import importlib_metadata as metadata
import pytest
import six


@pytest.mark.skipif(six.PY2, reason="Only makes sense for Python 3 installs")
def test_entrypoints():
    """Make sure all console_scripts defined by DIRAC are importable."""
    errors = []
    for ep in metadata.entry_points(group="console_scripts"):  # pylint: disable=unexpected-keyword-arg
        if ep.module.startswith("DIRAC"):
            try:
                ep.load()
            except ModuleNotFoundError as e:  # pylint: disable=undefined-variable
                errors.append(str(e))
    assert not errors, errors
