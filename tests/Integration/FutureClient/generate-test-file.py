#!/usr/bin/env python
import argparse
import inspect
import importlib
from itertools import zip_longest

BASE_EXPORTS = {"export_whoami", "export_refreshConfiguration", "export_ping", "export_echo"}


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate pytest stubs for testing the future client of a DIRAC system component"
    )
    parser.add_argument("system", help="DIRAC system name")
    parser.add_argument("component", help="DIRAC component name")
    args = parser.parse_args()
    main(args.system, args.component)


def main(system, component):
    client_name = f"{component}Client"
    handler = getattr(
        importlib.import_module(f"DIRAC.{system}System.Service.{component}Handler"), f"{component}Handler"
    )

    print("from functools import partial")
    print()
    print("import pytest")
    print()
    print("import DIRAC")
    print("DIRAC.initialize()")
    print(f"from DIRAC.{system}System.Client.{client_name} import {client_name}")
    print("from ..utils import compare_results")
    print()
    print()

    for export in dir(handler):
        if not export.startswith("export_") or export in BASE_EXPORTS:
            continue
        method_name = export[len("export_") :]
        types = getattr(handler, f"types_{method_name}")
        signature = inspect.signature(getattr(handler, export))
        type_infos = []
        for parameter, dtype in zip_longest(signature.parameters.values(), types):
            if dtype is None:
                dtype = ""
            elif isinstance(dtype, (list, tuple)):
                dtype = f": {' | '.join([d.__name__ for d in dtype])}"
            else:
                dtype = f": {dtype.__name__}"
            if parameter.default == inspect._empty:
                default = ""
            else:
                default = f" = {parameter.default}"
            type_infos += [f"{parameter.name}{dtype}{default}"]
        print(f"def test_{method_name}(monkeypatch):")
        print(f"    # {client_name}().{method_name}({', '.join(type_infos)})")
        print(f"    method = {client_name}().{method_name}")
        print(f"    pytest.skip()")
        print()
        print()


if __name__ == "__main__":
    parse_args()
