# DIRACCommon

Stateless utilities extracted from DIRAC for use by DiracX and other projects without triggering DIRAC's global state initialization.

## Purpose

This package solves the circular dependency issue where DiracX needs DIRAC utilities but importing DIRAC triggers global state initialization. DIRACCommon contains only stateless utilities that can be safely imported without side effects.

## Contents

- `DIRACCommon.Core.Utilities.ReturnValues`: DIRAC's S_OK/S_ERROR return value system
- `DIRACCommon.Core.Utilities.DErrno`: DIRAC error codes and utilities
- `DIRACCommon.Core.Utilities.ClassAd.ClassAdLight`: JDL parsing utilities
- `DIRACCommon.Core.Utilities.TimeUtilities`: Time and date utilities
- `DIRACCommon.Core.Utilities.StateMachine`: State machine utilities
- `DIRACCommon.Core.Utilities.JDL`: JDL parsing utilities
- `DIRACCommon.Core.Utilities.List`: List manipulation utilities
- `DIRACCommon.ConfigurationSystem.Client.Helpers.Resources`: Platform compatibility utilities
- `DIRACCommon.WorkloadManagementSystem.Client.JobStatus`: Job status constants and state machines
- `DIRACCommon.WorkloadManagementSystem.Client.JobState.JobManifest`: Job manifest utilities
- `DIRACCommon.WorkloadManagementSystem.DB.JobDBUtils`: Job database utilities
- `DIRACCommon.WorkloadManagementSystem.Utilities.JobModel`: Pydantic-based job models
- `DIRACCommon.WorkloadManagementSystem.Utilities.JobStatusUtility`: Job status utilities
- `DIRACCommon.WorkloadManagementSystem.Utilities.ParametricJob`: Parametric job utilities

## Installation

```bash
pip install DIRACCommon
```

## Usage

### Basic Usage

```python
from DIRACCommon.Core.Utilities.ReturnValues import S_OK, S_ERROR

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

## Migrating Code to DIRACCommon

This section documents the proper pattern for moving code from DIRAC to DIRACCommon to enable shared usage by DiracX and other projects.

### Migration Pattern

The migration follows a specific pattern to maintain backward compatibility while making code stateless:

1. **Move core functionality to DIRACCommon** - Create the stateless version
2. **Update DIRAC module** - Make it a backward compatibility wrapper
3. **Move and update tests** - Ensure both versions are tested
4. **Verify migration** - Test both import paths work correctly

### Step-by-Step Migration Process

#### 1. Create DIRACCommon Module

Create the new module in DIRACCommon with the **exact same directory structure** as DIRAC:

```bash
# Example: Moving from src/DIRAC/ConfigurationSystem/Client/Helpers/Resources.py
# Create: dirac-common/src/DIRACCommon/ConfigurationSystem/Client/Helpers/Resources.py
```

#### 2. Make Code Stateless

**❌ Remove these dependencies:**
```python
# DON'T import these in DIRACCommon
from DIRAC import gConfig, gLogger, gMonitor, Operations
from DIRAC.Core.Security import getProxyInfo
# Any other DIRAC global state
```

**✅ Use these instead:**
```python
# Use DIRACCommon's own utilities
from DIRACCommon.Core.Utilities.ReturnValues import S_OK, S_ERROR
from DIRACCommon.Core.Utilities.DErrno import strerror

# Accept configuration data as parameters
def my_function(data, config_dict):
    # Use config_dict instead of gConfig.getOptionsDict()
    pass
```

#### 3. Handle Configuration Data

**❌ Don't do this:**
```python
# DIRACCommon function taking config object
def getDIRACPlatform(OSList, config):
    result = config.getOptionsDict("/Resources/Computing/OSCompatibility")
    # ...
```

**✅ Do this instead:**
```python
# DIRACCommon function taking configuration data directly
def getDIRACPlatform(osList: str | list[str], osCompatibilityDict: dict[str, set[str]]) -> DReturnType[list[str]]:
    if not osCompatibilityDict:
        return S_ERROR("OS compatibility info not found")
    # Use osCompatibilityDict directly
    # ...
```

#### 4. Update DIRAC Module for Backward Compatibility

Transform the original DIRAC module into a backward compatibility wrapper:

```python
"""Backward compatibility wrapper - moved to DIRACCommon

This module has been moved to DIRACCommon.ConfigurationSystem.Client.Helpers.Resources
to avoid circular dependencies and allow DiracX to use these utilities without
triggering DIRAC's global state initialization.

All exports are maintained for backward compatibility.
"""

# Re-export everything from DIRACCommon for backward compatibility
from DIRACCommon.ConfigurationSystem.Client.Helpers.Resources import (
    getDIRACPlatform as _getDIRACPlatform,
    _platformSortKey,
)

from DIRAC import S_ERROR, S_OK, gConfig

def getDIRACPlatform(OSList):
    """Get standard DIRAC platform(s) compatible with the argument.

    Backward compatibility wrapper that uses gConfig.
    """
    result = gConfig.getOptionsDict("/Resources/Computing/OSCompatibility")
    if not (result["OK"] and result["Value"]):
        return S_ERROR("OS compatibility info not found")

    # Convert string values to sets for DIRACCommon function
    platformsDict = {k: set(v.replace(" ", "").split(",")) for k, v in result["Value"].items()}
    for k, v in platformsDict.items():
        if k not in v:
            v.add(k)

    return _getDIRACPlatform(OSList, platformsDict)

# Re-export the helper function
_platformSortKey = _platformSortKey
```

#### 5. Move and Update Tests

**Create DIRACCommon tests:**
```python
# dirac-common/tests/ConfigurationSystem/Client/Helpers/test_Resources.py
from DIRACCommon.ConfigurationSystem.Client.Helpers.Resources import getDIRACPlatform

def test_getDIRACPlatform():
    # Test with configuration data directly
    osCompatibilityDict = {
        "plat1": {"OS1", "OS2"},
        "plat2": {"OS3", "OS4"}
    }
    result = getDIRACPlatform("OS1", osCompatibilityDict)
    assert result["OK"]
    assert "plat1" in result["Value"]
```

**Update DIRAC tests:**
```python
# src/DIRAC/ConfigurationSystem/Client/Helpers/test/Test_Helpers.py
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getDIRACPlatform

def test_getDIRACPlatform():
    # Test backward compatibility wrapper
    # (existing tests should continue to work)
    pass
```

#### 6. Create Directory Structure

Ensure the DIRACCommon directory structure mirrors DIRAC exactly:

```bash
dirac-common/src/DIRACCommon/
├── ConfigurationSystem/
│   ├── __init__.py
│   └── Client/
│       ├── __init__.py
│       └── Helpers/
│           ├── __init__.py
│           └── Resources.py
└── tests/
    └── ConfigurationSystem/
        ├── __init__.py
        └── Client/
            ├── __init__.py
            └── Helpers/
                ├── __init__.py
                └── test_Resources.py
```

### Requirements for DIRACCommon Code

Code in DIRACCommon **MUST** be:

- **Completely stateless** - No global variables or state
- **No DIRAC dependencies** - Cannot import from DIRAC
- **No global state access** - Cannot use `gConfig`, `gLogger`, `gMonitor`, etc.
- **No database connections** - Cannot establish DB connections
- **No side effects on import** - Importing should not trigger any initialization
- **Pure functions** - Functions should be deterministic and side-effect free

### Configuration Data Handling

When DIRACCommon functions need configuration data:

1. **Accept data as parameters** - Don't accept config objects
2. **Use specific data types** - Pass dictionaries, not config objects
3. **Let DIRAC wrapper handle gConfig** - DIRAC gets data and passes it to DIRACCommon

### Example Migration

See the migration of `getDIRACPlatform` in:
- `dirac-common/src/DIRACCommon/ConfigurationSystem/Client/Helpers/Resources.py`
- `src/DIRAC/ConfigurationSystem/Client/Helpers/Resources.py`

This demonstrates the complete pattern from stateless DIRACCommon implementation to backward-compatible DIRAC wrapper.

### Testing the Migration

After migration, verify:

1. **DIRACCommon tests pass** - `pixi run python -m pytest dirac-common/tests/`
2. **DIRAC tests pass** - `pixi run python -m pytest src/DIRAC/`
3. **Both import paths work**:
   ```python
   # DIRACCommon (stateless)
   from DIRACCommon.ConfigurationSystem.Client.Helpers.Resources import getDIRACPlatform

   # DIRAC (backward compatibility)
   from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getDIRACPlatform
   ```
4. **No linting errors** - All code should pass linting checks
