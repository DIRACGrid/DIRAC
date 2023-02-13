import importlib_metadata as metadata


def test_entrypoints():
    """Make sure all console_scripts defined by DIRAC are importable."""
    errors = []
    for ep in metadata.entry_points(group="console_scripts"):
        if ep.module.startswith("DIRAC"):
            try:
                ep.load()
            except ModuleNotFoundError as e:
                errors.append(str(e))
    assert not errors, errors
