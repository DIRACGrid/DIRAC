""" The Pilots_Command class is a command class to know about 
    present pilots efficiency
"""

from DIRAC.ResourceStatusSystem.Client.Command.Command import Command
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.ResourceStatusSystem.Utilities.Utils import *

#############################################################################

class RSPeriods_Command(Command):
  
  def doCommand(self, args, clientIn=None):
    """ Return getPeriods from ResourceStatus Client
    
        - args[0] should be a ValidRes
        - args[1] should be the name of the ValidRes
        - args[2] should be the present status
        - args[3] are the number of hours requested

    """

    if not isinstance(args, tuple):
      raise TypeError, where(self, self.doCommand)
    
    if args[0] not in ValidRes:
      raise InvalidRes, where(self, self.doCommand)
    
    if clientIn is not None:
      c = clientIn
    else:
      from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient   
      c = ResourceStatusClient()
      
    return c.getPeriods(args[0], args[1], args[2], args[3])

