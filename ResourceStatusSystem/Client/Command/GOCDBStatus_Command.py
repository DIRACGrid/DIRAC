""" The GOCDBStatus_Command class is a command class to know about 
    present downtimes
"""

from DIRAC.ResourceStatusSystem.Client.Command.Command import Command
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.ResourceStatusSystem.Utilities.Utils import *

#############################################################################

class GOCDBStatus_Command(Command):
  
  def doCommand(self, args, clientIn=None):
    """ Return getStatus from GOC DB Client
    
       :params:
         :attr:`args`: 
           - args[0]: string: should be a ValidRes
      
           - args[1]: string: should be the name of the ValidRes

           - args[2]: string: optional, number of hours in which 
           the down time is starting
    """

    if not isinstance(args, tuple):
      raise TypeError, where(self, self.doCommand)
    
    if args[0] not in ValidRes:
      raise InvalidRes, where(self, self.doCommand)
    
    if clientIn is not None:
      c = clientIn
    else:
      # use standard GOC DB Client
      from DIRAC.ResourceStatusSystem.Client.GOCDBClient import GOCDBClient   
      c = GOCDBClient()
      
    if len(args) == 2:
      res = c.getStatus(args[0], args[1])[0]
    elif len(args) == 3:
      res = c.getStatus(args[0], args[1], None, args[2])[0]

    if 'url' in res.keys():
      del res['url']

    return res

#############################################################################

class GOCDBInfo_Command(Command):
  
  def doCommand(self, args, clientIn=None):
    """ Return all info from GOC DB Client
    
       :params:
         :attr:`args`: 
           - args[0]: string: should be a ValidRes
      
           - args[1]: string: should be the name of the ValidRes
    """

    if not isinstance(args, tuple):
      raise TypeError, where(self, self.doCommand)
    
    if args[0] not in ValidRes:
      raise InvalidRes, where(self, self.doCommand)
    
    if clientIn is not None:
      c = clientIn
    else:
      # use standard GOC DB Client
      from DIRAC.ResourceStatusSystem.Client.GOCDBClient import GOCDBClient   
      c = GOCDBClient()
      
    if len(args) == 2:
      res = c.getStatus(args[0], args[1])[0]
    elif len(args) == 3:
      res = c.getStatus(args[0], args[1], None, args[2])[0]

    return res

#############################################################################
    