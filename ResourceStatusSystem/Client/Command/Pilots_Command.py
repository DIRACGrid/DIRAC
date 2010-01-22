""" The Pilots_Command class is a command class to know about 
    present pilots efficiency
"""

from DIRAC.ResourceStatusSystem.Client.Command.Command import Command
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.ResourceStatusSystem.Utilities.Utils import *

#############################################################################

class PilotsStats_Command(Command):
  
  def doCommand(self, args, clientIn=None):
    """ Return getPilotStats from Pilots Client  
    """

    if not isinstance(args, tuple):
      raise TypeError, where(self, self.doCommand)
    
    if args[0] not in ValidRes:
      raise InvalidRes, where(self, self.doCommand)
    
    if clientIn is not None:
      c = clientIn
    else:
      from DIRAC.ResourceStatusSystem.Client.PilotsClient import PilotsClient   
      c = PilotsClient()
      
    return c.getPilotsStats(args[0], args[1], args[2])


#############################################################################

class PilotsEff_Command(Command):
  
  def doCommand(self, args, clientIn=None):
    """ Return getPilotsEff from Pilots Client  
    """

    if not isinstance(args, tuple):
      raise TypeError, where(self, self.doCommand)
    
    if args[0] not in ValidRes:
      raise InvalidRes, where(self, self.doCommand)
    
    if clientIn is not None:
      c = clientIn
    else:
      from DIRAC.ResourceStatusSystem.Client.PilotsClient import PilotsClient   
      c = PilotsClient()
      
    return c.getPilotsEff(args[0], args[1], args[2])

#############################################################################

class PilotsEffSimple_Command(Command):
  
  def doCommand(self, args, clientIn=None):
    """ Returns simple pilots efficiency
    
        :params:
          :attr:`args`:
            - args[0]: string - should be a ValidRes
            
            - args[1]: string - should be the name of the ValidRes

        returns:
          {
            'pilotsEff': 'Good'|'Fair'|'Poor'|'Idle'|'Bad'
          }
    """

    if args[0] not in ValidRes:
      raise InvalidRes, where(self, self.doCommand)
    
    if not isinstance(args, tuple):
      raise TypeError, where(self, self.doCommand)
    
    if clientIn is not None:
      c = clientIn
    else:
      from DIRAC.ResourceStatusSystem.Client.PilotsClient import PilotsClient   
      c = PilotsClient()
      
    return c.getPilotsSimpleEff(args[0], args[1])

#############################################################################
