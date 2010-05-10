""" The Jobs_Command class is a command class to know about 
    present jobs efficiency
"""

from DIRAC import gLogger

from DIRAC.ResourceStatusSystem.Client.Command.Command import Command
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *

#############################################################################

class JobsEffSimpleEveryOne_Command(Command):
  
  def doCommand(self, sites = None):
    """ 
    Returns simple jobs efficiency for all the sites in input.
        
    :params:
      :attr:`sites`: list of site names (when not given, take every site)
    
      :attr:`RPCWMSAdmin`: optional RPCClient to RPCWMSAdmin

    :returns:
      {'SiteName': 'Good'|'Fair'|'Poor'|'Idle'|'Bad'}
    """
    
    if self.client is None:
      from DIRAC.ResourceStatusSystem.Client.JobsClient import JobsClient   
      self.client = JobsClient()

    if sites is None:
      from DIRAC.Core.DISET.RPCClient import RPCClient
      RPC = RPCClient("ResourceStatus/ResourceStatus")
      sites = RPC.getSitesList()
      if not sites['OK']:
        raise RSSException, where(self, self.doCommand) + " " + sites['Message'] 
      else:
        sites = sites['Value']
    
    if self.RPC is None:
      from DIRAC.Core.DISET.RPCClient import RPCClient
      self.RPC = RPCClient("WorkloadManagement/WMSAdministrator")
    
    try:
      res = self.client.getJobsSimpleEff(sites, self.RPC)
    except:
      gLogger.exception("Exception when calling JobsClient.")
      return {'Result':'Unknown'}
    
    return res

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__


#############################################################################

class PilotsEffSimpleEverySites_Command(Command):
  
  def doCommand(self, sites = None):
    """ 
    Returns simple pilots efficiency for all the sites and resources in input.
        
    :params:
      :attr:`sites`: list of site names (when not given, take every site)
    
      :attr:`RPCWMSAdmin`: optional RPCClient to RPCWMSAdmin

    :returns:
      {'SiteName': 'Good'|'Fair'|'Poor'|'Idle'|'Bad'}
    """
    
    if self.client is None:
      from DIRAC.ResourceStatusSystem.Client.PilotsClient import PilotsClient   
      self.client = PilotsClient()

    if sites is None:
      from DIRAC.Core.DISET.RPCClient import RPCClient
      RPC = RPCClient("ResourceStatus/ResourceStatus")
      sites = RPC.getSitesList()
      if not sites['OK']:
        raise RSSException, where(self, self.doCommand) + " " + res['Message'] 
      else:
        sites = sites['Value']
    
    if self.RPC is None:
      from DIRAC.Core.DISET.RPCClient import RPCClient
      self.RPC = RPCClient("WorkloadManagement/WMSAdministrator")
    
    try:
      res = self.client.getPilotsSimpleEff('Site', sites, None, self.RPC)
    except:
      gLogger.exception("Exception when calling PilotsClient.")
      return {'Result':'Unknown'}
    
    return res

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__


#############################################################################
