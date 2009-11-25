""" 
The Propagation_Command module maintains command classes for gaining 
information on status of all kind of valid resources for propagation purpose
"""

from DIRAC.ResourceStatusSystem.Client.Command.Command import Command
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.ResourceStatusSystem.Utilities.Utils import *

#############################################################################

class ServiceStats_Command(Command):
  """ 
  The ServiceStats_Command class is a command class to know about 
  present services stats
  """
  
  def doCommand(self, args, clientIn=None):
    """ 
    Uses :meth:`DIRAC.ResourceStatusSystem.Client.ResourceStatusClient.getServiceStats`  

    :params:
      :attr:`args`: a tuple
        - `args[0]` should be the name of the Site
        
    :returns:
      {'Active':xx, 'Probing':yy, 'Banned':zz, 'Total':xyz}
    """

    if not isinstance(args, tuple):
      raise TypeError, where(self, self.doCommand)
    
    if clientIn is not None:
      c = clientIn
    else:
      from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient   
      c = ResourceStatusClient()
      
    return c.getServiceStats(args[0])

#############################################################################

class ResourceStats_Command(Command):
  """ 
  The ResourceStats_Command class is a command class to know about 
  present resources stats
  """
  
  def doCommand(self, args, clientIn=None):
    """ 
    Uses :meth:`DIRAC.ResourceStatusSystem.Client.ResourceStatusClient.getResourceStats`  

    :params:
      :attr:`args`: a tuple
        - `args[0]` should be in ['Site', 'Service']

        - `args[1]` should be the name of the Site or Service
        
    :returns:
    
    """

    if not isinstance(args, tuple):
      raise TypeError, where(self, self.doCommand)
    
    if args[0] not in ['Site', 'Service']:
      raise InvalidRes
    
    if clientIn is not None:
      c = clientIn
    else:
      from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient   
      c = ResourceStatusClient()
      
    return c.getResourceStats(args[0], args[1])

#############################################################################
