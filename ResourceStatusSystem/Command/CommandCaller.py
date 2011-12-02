################################################################################
# $HeadURL $
################################################################################
__RCSID__ = "$Id:  $"

from DIRAC.ResourceStatusSystem.Command.ClientsInvoker import ClientsInvoker
from DIRAC.ResourceStatusSystem.Utilities.CS import getExt

class CommandCaller:

  """
    Module used for calling policies. Its class is used for invoking
    real policies, based on the policy name
  """

  def commandInvocation(self, granularity = None, name = None, command = None,  
                        args = None, comm = None, extraArgs = None):
    
    c = command   
    if c is None: 
      c = self.setCommandObject(comm)

    a = (granularity, name)
    if extraArgs is not None:
      a = a + extraArgs

    res = self._innerCall(c, a)
    return res

################################################################################

  def setCommandObject( self, comm ):
    """ 
    Returns a command object, given comm
    
    :params:
      `comm`: a tuple, where comm[0] is a module name and comm[1] is a class name (inside the module)
    """ 

    moduleBase = "DIRAC.ResourceStatusSystem.Command."
    
    ext = getExt()
    
    # TRY FIRST TO IMPORT FROM DIRAC. AS IT IS NOW, THERE ARE MUCH MORE COMMANDS IN
    # DIRAC THAN IN THE EXTENSION. IT MIGHT CHANGE.
    
    try:
      cModule = comm[0]
      cClass = comm[1]
      module = moduleBase + cModule
      commandModule = __import__(module, globals(), locals(), ['*'])
    except ImportError:  
      try:
        cModule = comm[0]
        cClass = comm[1]
        module = ext + moduleBase + cModule
        commandModule = __import__(module, globals(), locals(), ['*'])
      except ImportError:
        cModule = "Command"
        cClass = "Command"
        module = moduleBase + cModule
        commandModule = __import__(module, globals(), locals(), ['*'])
      
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