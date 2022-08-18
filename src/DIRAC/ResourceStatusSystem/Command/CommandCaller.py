""" CommandCaller

  Module that loads commands and executes them.

"""
import copy

from DIRAC import S_ERROR, S_OK
from DIRAC.ResourceStatusSystem.Utilities import Utils


def commandInvocation(commandTuple, pArgs=None, decisionParams=None, clients=None):
    """
    Returns a command object, given commandTuple

    :params:
      `commandTuple`: a tuple, where commandTuple[0] is a module name and
      commandTuple[1] is a class name (inside the module)
    """

    if commandTuple is None:
        return S_OK(None)

    # decision params can be a dictionary passed with all the element parameters
    # used mostly by the PDP to inject all relevant information
    if decisionParams is None:
        decisionParams = {}

    # arguments hardcoded on Configurations.py for the policy
    if pArgs is None:
        pArgs = {}

    try:
        cModule = commandTuple[0]
        cClass = commandTuple[1]
        commandModule = Utils.voimport("DIRAC.ResourceStatusSystem.Command." + cModule)
    except ImportError:
        return S_ERROR("Import error for command %s." % (cModule))

    if not hasattr(commandModule, cClass):
        return S_ERROR(f"{cModule} has no {cClass}")

    # We merge decision parameters and policy arguments.
    newArgs = copy.deepcopy(decisionParams)
    newArgs.update(pArgs)

    commandObject = getattr(commandModule, cClass)(newArgs, clients)

    return S_OK(commandObject)
