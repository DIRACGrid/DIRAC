########################################################################
# $HeadURL$
########################################################################

""" The SystemAdministratorIntegrator is a class integrating access to all the
    SystemAdministrator services configured in the system
"""

__RCSID__ = "$Id$"

from DIRAC.FrameworkSystem.Client.SystemAdministratorClient import SystemAdministratorClient
import DIRAC.ConfigurationSystem.Client.Helpers.Registry as Registry
from DIRAC.Core.Utilities.ThreadPool import ThreadPool
from DIRAC import S_OK

SYSADMIN_PORT = 9162

class SystemAdministratorIntegrator:

  def __init__( self, **kwargs ):
    """ Constructor  
    """
    if 'hosts' in kwargs:
      self.__hosts = kwargs['hosts']
      del kwargs['hosts']
    else:  
      result = Registry.getHosts()
      if result['OK']:
        self.__hosts = result['Value']
      else:
        self.__hosts = []
      
    self.__kwargs = dict( kwargs )  
    self.__pool = ThreadPool( len( self.__hosts ) )  
    self.__resultDict = {}
      
  def __getattr__( self, name ):
    self.call = name
    return self.execute

  def __executeClient( self, host, method, *parms, **kwargs ):
    """ Execute RPC method on a given host 
    """        
    hostName = Registry.getHostOption( host, 'Host', host)
    client = SystemAdministratorClient( hostName, **self.__kwargs )
    result = getattr( client, method )( *parms, **kwargs )
    result['Host'] = host   
    return result
    
  def __processResult( self, id_, result ):
    """ Collect results in the final structure
    """
    host = result['Host']
    del result['Host']
    self.__resultDict[host] = result  
       
  def execute(self, *args, **kwargs ):
    """ Main execution method
    """
    self.__resultDict = {}
    for host in self.__hosts:
      self.__pool.generateJobAndQueueIt( self.__executeClient,
                                         args = [ host, self.call ] + list(args),
                                         kwargs = kwargs,
                                         oCallback = self.__processResult )
    
    self.__pool.processAllResults()
    return S_OK( self.__resultDict )    