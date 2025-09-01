""" :mod: DErrno

    ==========================

    .. module: DErrno

    :synopsis: Error list and utilities for handling errors in DIRAC


    This module contains list of errors that can be encountered in DIRAC.
    It complements the errno module of python.

    It also contains utilities to manipulate these errors.

    Finally, it contains a DErrno class that contains an error number
    as well as a low level error message. It behaves like a string for
    compatibility reasons

    In order to add extension specific error, you need to create in your extension the file
    Core/Utilities/DErrno.py, which will contain the following dictionary:

      * extra_dErrName: keys are the error name, values the number of it
      * extra_dErrorCode: same as dErrorCode. keys are the error code, values the name
                          (we don't simply revert the previous dict in case we do not
                          have a one to one mapping)
      * extra_dStrError: same as dStrError, Keys are the error code, values the error description
      * extra_compatErrorString: same as compatErrorString. The compatible error strings are
                                 added to the existing one, and not replacing them.


    Example of extension file :

       * extra_dErrName = { 'ELHCBSPE' : 3001 }
       * extra_dErrorCode = { 3001 : 'ELHCBSPE'}
       * extra_dStrError = { 3001 : "This is a description text of the specific LHCb error" }
       * extra_compatErrorString = { 3001 : ["living easy, living free"],
                             DErrno.ERRX : ['An error message for ERRX that is specific to LHCb']}

"""
import importlib
import sys

# Import all the stateless parts from DIRACCommon
from DIRACCommon.Core.Utilities.DErrno import *  # noqa: F401, F403

from DIRAC.Core.Utilities.Extensions import extensionsByPriority

# compatErrorString is used by the extension mechanism but not in DIRACCommon
compatErrorString = {}


def includeExtensionErrors():
    """Merge all the errors of all the extensions into the errors of these modules
    Should be called only at the initialization of DIRAC, so by the parseCommandLine,
    dirac-agent.py, dirac-service.py, dirac-executor.py
    """
    for extension in reversed(extensionsByPriority()):
        if extension == "DIRAC":
            continue
        try:
            ext_derrno = importlib.import_module(f"{extension}.Core.Utilities.DErrno")
        except ImportError:
            pass
        else:
            # The next 3 dictionary MUST be present for consistency
            # Global name of errors
            sys.modules[__name__].__dict__.update(ext_derrno.extra_dErrName)
            # Dictionary with the error codes
            sys.modules[__name__].dErrorCode.update(ext_derrno.extra_dErrorCode)
            # Error description string
            sys.modules[__name__].dStrError.update(ext_derrno.extra_dStrError)

            # extra_compatErrorString is optional
            for err in getattr(ext_derrno, "extra_compatErrorString", []):
                sys.modules[__name__].compatErrorString.setdefault(err, []).extend(
                    ext_derrno.extra_compatErrorString[err]
                )
