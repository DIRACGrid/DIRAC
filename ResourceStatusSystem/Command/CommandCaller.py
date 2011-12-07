################################################################################
# $HeadURL $
################################################################################
__RCSID__ = "$Id:  $"

from DIRAC import gLogger
from DIRAC.ResourceStatusSystem.Utilities              import Utils
from DIRAC.ResourceStatusSystem.Command.ClientsInvoker import ClientsInvoker

class CommandCaller:
  """
    Module used for calling policies. Its class is used for invoking
    real policies, based on the policy name
  """

  def commandInvocation(self, granularity = None, name = None, command = None,
                        args = None, comm = None, extraArgs = None):

    c = command if command else self.setCommandObject(comm)
    a = (granularity, name) if not extraArgs else (granularity, name) + extraArgs

    res = self._innerCall(c, a)
    return res

################################################################################

  def setCommandObject( self, comm ):
    """
    Returns a command object, given comm

    :params:
      `comm`: a tuple, where comm[0] is a module name and comm[1] is a class name (inside the module)
    """
    try:
      cModule = comm[0]
      cClass = comm[1]
      commandModule = Utils.voimport("DIRAC.ResourceStatusSystem.Command." + cModule)
    except ImportError:
      gLogger.warn("Command %s/%s not found, using dummy command DoNothing_Command." % (cModule, cClass))
      cClass = "DoNothing_Command"
      commandModule = __import__("DIRAC.ResourceStatusSystem.Command.DoNothing_Command", globals(), locals(), ['*'])

    c = getattr(commandModule, cClass)()

    return c

################################################################################

  def setAPI( self, cObj, apiName, apiInstance ):
    cObj.setAPI( apiName, apiInstance )

################################################################################

  def _innerCall(self, c, a):#, clientIn = None):
    """ command call
    """
    clientsInvoker = ClientsInvoker()

    c.setArgs(a)
    clientsInvoker.setCommand(c)

    res = clientsInvoker.doCommand()

    return res

################################################################################
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
################################################################################

'''
  HOW DOES THIS WORK.

    will come soon...
'''

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
