""" Storage plug-ins related utilities
"""

import errno

from typing import Union, TypeVar, overload, Literal
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities.ReturnValues import DReturnType

T = TypeVar("T")


# mypy doesn't understand default parameter values with generics so use overloads (python/mypy#3737)
@overload
def checkArgumentFormat(path: Union[str, list[str]]) -> DReturnType[dict[str, Literal[False]]]:
    ...


@overload
def checkArgumentFormat(path: dict[str, T]) -> DReturnType[dict[str, T]]:
    ...


def checkArgumentFormat(path):  # type: ignore
    """returns {'/this/is/an/lfn.1':False, '/this/is/an/lfn.2':False ...}"""

    if isinstance(path, str):
        return S_OK({path: False})
    elif isinstance(path, list):
        return S_OK({url: False for url in path if isinstance(url, str)})
    elif isinstance(path, dict):
        returnDict = path.copy()
        return S_OK(returnDict)
    else:
        return S_ERROR(errno.EINVAL, "Utils.checkArgumentFormat: Supplied path is not of the correct format.")
