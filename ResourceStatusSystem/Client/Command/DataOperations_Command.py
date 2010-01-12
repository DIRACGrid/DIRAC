""" The DataOperations_Command module is a module to know about data operations stats
"""

from DIRAC.ResourceStatusSystem.Client.Command.Command import Command
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.ResourceStatusSystem.Utilities.Utils import *

class TransferQuality_Command(Command):
  
  def doCommand(self, args, clientIn=None):
    """ Return getQuality from Data Operations Client
    
        :params:
          :attr:`args`: a tuple. It has to contains just the SE name (a string),
          and optionally two datetime for from and to dates.
    """

    if not isinstance(args, tuple):
      raise TypeError, where(self, self.doCommand)
    
    if len(args) == 1:
      args = (args[0], None, None)
    if len(args) == 2:
      args = (args[0], args[1], None)
    
    if clientIn is not None:
      doc = clientIn
    else:
      # use standard GOC DB Client
      from DIRAC.ResourceStatusSystem.Client.DataOperationsClient import DataOperationsClient   
      doc = DataOperationsClient()
      
    return doc.getQualityStats(args[0], args[1], args[2])
