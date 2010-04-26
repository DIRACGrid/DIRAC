""" The Pilots_Command class is a command class to know about 
    present pilots efficiency
"""

from DIRAC import gLogger

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
      
    try:
      res = c.getPilotsStats(args[0], args[1], args[2])
    except:
      gLogger.exception("Exception when calling PilotsClient for %s %s" %(args[0], args[1]))
      return {'PilotsStats':'Unknown'}
  
    return {'PilotsStats':res}


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
    
    try:  
      res = c.getPilotsEff(args[0], args[1], args[2])
    except:
      gLogger.exception("Exception when calling PilotsClient for %s %s" %(args[0], args[1]))
      return {'PilotsEff':'Unknown'}
  
    return {'PilotsEff':res}

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

    if args[0] in ('Service', 'Services'):
      from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient   
      c = ResourceStatusClient()
      try:
        name = c.getGeneralName(args[0], args[1], 'Site')
      except:
        gLogger.error("Can't get a general name for %s %s" %(args[0], args[1]))
        return {'PilotsEff':'Unknown'}      
      granularity = 'Site'
    elif args[0] in ('Site', 'Sites', 'Resource', 'Resources'):
      name = args[1]
      granularity = args[0]
    else:
      raise InvalidRes, where(self, self.doCommand)
    
    if not isinstance(args, tuple):
      raise TypeError, where(self, self.doCommand)
    
    if clientIn is not None:
      c = clientIn
    else:
      from DIRAC.ResourceStatusSystem.Client.PilotsClient import PilotsClient   
      c = PilotsClient()
      
    try:
      res = c.getPilotsSimpleEff(granularity, name)
    except:
      gLogger.exception("Exception when calling PilotsClient for %s %s" %(granularity, name))
      return {'PilotsEff':'Unknown'}
    
    return {'PilotsEff':res} 

#############################################################################
