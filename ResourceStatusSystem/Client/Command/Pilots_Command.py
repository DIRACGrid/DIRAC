""" The Pilots_Command class is a command class to know about 
    present pilots efficiency
"""

from DIRAC import gLogger

from DIRAC.ResourceStatusSystem.Client.Command.Command import Command
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *

#############################################################################

class PilotsStats_Command(Command):
  
  def doCommand(self):
    """ 
    Return getPilotStats from Pilots Client  
    """

    if self.client is None:
      from DIRAC.ResourceStatusSystem.Client.PilotsClient import PilotsClient   
      self.client = PilotsClient()
      
    try:
      res = self.client.getPilotsStats(self.args[0], self.args[1], self.args[2])
    except:
      gLogger.exception("Exception when calling PilotsClient for %s %s" %(self.args[0], self.args[1]))
      return {'Result':'Unknown'}
  
    return {'Result':res}

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__
    
#############################################################################

class PilotsEff_Command(Command):
  
  def doCommand(self):
    """ 
    Return getPilotsEff from Pilots Client  
    """
    
    if self.client is None:
      from DIRAC.ResourceStatusSystem.Client.PilotsClient import PilotsClient   
      self.client = PilotsClient()
    
    try:  
      res = self.client.getPilotsEff(self.args[0], self.args[1], self.args[2])
    except:
      gLogger.exception("Exception when calling PilotsClient for %s %s" %(self.args[0], self.args[1]))
      return {'Result':'Unknown'}
  
    return {'Result':res}

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__
    
#############################################################################

class PilotsEffSimple_Command(Command):
  
  def doCommand(self, RSClientIn = None):
    """ 
    Returns simple pilots efficiency
    
    :attr:`args`:
        - args[0]: string - should be a ValidRes
        
        - args[1]: string - should be the name of the ValidRes

    returns:
      {
        'pilotsEff': 'Good'|'Fair'|'Poor'|'Idle'|'Bad'
      }
    """

    if self.args[0] in ('Service', 'Services'):
      if RSClientIn is not None:
        rsc = RSClientIn
      else:
        from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient   
        rsc = ResourceStatusClient()

      try:
        name = rsc.getGeneralName(self.args[0], self.args[1], 'Site')
      except:
        gLogger.error("Can't get a general name for %s %s" %(self.args[0], self.args[1]))
        return {'PilotsEff':'Unknown'}      
      granularity = 'Site'
    
    elif self.args[0] in ('Site', 'Sites', 'Resource', 'Resources'):
      name = self.args[1]
      granularity = self.args[0]
    else:
      raise InvalidRes, where(self, self.doCommand)
    
    if self.client is None:
      from DIRAC.ResourceStatusSystem.Client.PilotsClient import PilotsClient   
      self.client = PilotsClient()
      
    try:
      res = self.client.getPilotsSimpleEff(granularity, name)
    except:
      gLogger.exception("Exception when calling PilotsClient for %s %s" %(granularity, name))
      return {'Result':'Unknown'}
    
    return {'Result':res[name]} 

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__
    
#############################################################################
