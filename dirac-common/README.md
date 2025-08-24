# DIRACCommon

Stateless utilities extracted from DIRAC for use by DiracX and other projects without triggering DIRAC's global state initialization.

## Purpose

This package solves the circular dependency issue where DiracX needs DIRAC utilities but importing DIRAC triggers global state initialization. DIRACCommon contains only stateless utilities that can be safely imported without side effects.

## Contents

- `DIRACCommon.Utils.ReturnValues`: DIRAC's S_OK/S_ERROR return value system
- `DIRACCommon.Utils.DErrno`: DIRAC error codes and utilities

## Installation

```bash
pip install DIRACCommon
```

## Usage

```python
from DIRACCommon.Utils.ReturnValues import S_OK, S_ERROR

def my_function():
    if success:
        return S_OK("Operation successful")
    else:
        return S_ERROR("Operation failed")
```

## Development

This package is part of the DIRAC project and shares its version number. When DIRAC is released, DIRACCommon is also released with the same version.

```bash
pixi install
pixi run pytest
```

## Guidelines for Adding Code

Code added to DIRACCommon must:
- Be completely stateless
- Not import or use any of DIRAC's global objects (`gConfig`, `gLogger`, `gMonitor`, `Operations`)
- Not establish database connections
- Not have side effects on import
