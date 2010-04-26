""" The DataOperations_Command module is a module to know about data operations stats
"""

from DIRAC import gLogger

from DIRAC.ResourceStatusSystem.Client.Command.Command import Command
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.ResourceStatusSystem.Utilities.Utils import *

class TransferQuality_Command(Command):
  
  def doCommand(self, args, clientIn=None):
    """ Return getQuality from Data Operations Client
    
        :params:
          :attr:`args`: a tuple
            - args[0]: string: should be a ValidRes
      
            - args[1]: string should be the name of the ValidRes

            - args[2]: optional dateTime object: a "from" date
          
            - args[3]: optional dateTime object: a "to" date
          
        :returns:
          {'TransferQuality': None| a number between 0 and 1}
    """

    if not isinstance(args, tuple):
      raise TypeError, where(self, self.doCommand)
    
    if len(args) == 2:
      args = (args[0], args[1], None, None)
    elif len(args) == 3:
      args = (args[0], args[1], args[2], None)
    elif len(args) == 4:
      pass
    else:
      raise RSSException, where(self, self.doCommand) + " args should have 2, 3 or 4 params"
    
    if clientIn is not None:
      doc = clientIn
    else:
      # use standard Client
      from DIRAC.ResourceStatusSystem.Client.DataOperationsClient import DataOperationsClient   
      doc = DataOperationsClient()
      
    try:
      res = doc.getQualityStats(args[0], args[1], args[2], args[3])
    except:
      gLogger.exception("Exception when calling DataOperationsClient for %s %s" %(args[0], args[1]))
      return {'TransferQuality':'Unknown'}
      
    
    return {'TransferQuality': res}
