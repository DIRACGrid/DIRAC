""" This is deprecated module, use DIRAC.Core.Utilities.DIRACScript.DIRACScript instead.
"""
from DIRAC.Core.Utilities.DIRACScript import DIRACScript
from DIRAC.Core.Utilities.Decorators import deprecated


@deprecated(
    "To create script use: from DIRAC.Core.Utilities.DIRACScript import DIRACScript as Script"
    "To load user configuration use: DIRAC.initialize()"
)
def parseCommandLine(*args, **kwargs):
    return DIRACScript.parseCommandLine(*args, **kwargs)


@deprecated("Please use: from DIRAC.Core.Utilities.DIRACScript import DIRACScript as Script")
def initialize(*args, **kwargs):
    return DIRACScript.initialize(*args, **kwargs)


@deprecated("Please use: from DIRAC.Core.Utilities.DIRACScript import DIRACScript as Script")
def registerSwitch(*args, **kwargs):
    return DIRACScript.registerSwitch(*args, **kwargs)


@deprecated("Please use: from DIRAC.Core.Utilities.DIRACScript import DIRACScript as Script")
def registerArgument(*args, **kwargs):
    return DIRACScript.registerArgument(*args, **kwargs)


@deprecated("Please use: from DIRAC.Core.Utilities.DIRACScript import DIRACScript as Script")
def getPositionalArgs(*args, **kwargs):
    return DIRACScript.getPositionalArgs(*args, **kwargs)


@deprecated("Please use: from DIRAC.Core.Utilities.DIRACScript import DIRACScript as Script")
def getExtraCLICFGFiles(*args, **kwargs):
    return DIRACScript.getExtraCLICFGFiles(*args, **kwargs)


@deprecated("Please use: from DIRAC.Core.Utilities.DIRACScript import DIRACScript as Script")
def getUnprocessedSwitches(*args, **kwargs):
    return DIRACScript.getUnprocessedSwitches(*args, **kwargs)


@deprecated("Please use: from DIRAC.Core.Utilities.DIRACScript import DIRACScript as Script")
def addDefaultOptionValue(*args, **kwargs):
    return DIRACScript.addDefaultOptionValue(*args, **kwargs)


@deprecated("Please use: from DIRAC.Core.Utilities.DIRACScript import DIRACScript as Script")
def setUsageMessage(*args, **kwargs):
    return DIRACScript.setUsageMessage(*args, **kwargs)


@deprecated("Please use: from DIRAC.Core.Utilities.DIRACScript import DIRACScript as Script")
def disableCS(*args, **kwargs):
    return DIRACScript.disableCS(*args, **kwargs)


@deprecated("Please use: from DIRAC.Core.Utilities.DIRACScript import DIRACScript as Script")
def enableCS(*args, **kwargs):
    return DIRACScript.enableCS(*args, **kwargs)
