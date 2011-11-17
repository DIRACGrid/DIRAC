########################################################################
# $HeadURL $
# File: FTSCleaningAgent.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2011/06/23 10:11:29
########################################################################

""" :mod: FTSCleaningAgent 
    =======================
 
    .. module: FTSCleaningAgent
    :synopsis: cleaning old FTS
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    cleaning old request in TransferDB, for which TURLS are the same 

    This agent should be run on host running or having direct access to 
    mysql server togehter with RequestDBMySQL client. 

"""

__RCSID__ = "$Id $"

##
# @file FTSCleaningAgent.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2011/06/23 10:15:04
# @brief Definition of FTSCleaningAgent class.

## imports 
import types
import inspect

from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Utilities.ThreadPool import ThreadPool, ThreadedJob
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.DataManagementSystem.private.RequestAgentBase import RequestAgentBase
from DIRAC.DataManagementSystem.private.FTSCurePlugin import FTSCurePlugin, injectFunction
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.RequestManagementSystem.Client.RequestClient import RequestClient
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from DIRAC.DataManagementSystem.Client.DataLoggingClient import DataLoggingClient
from DIRAC.DataManagementSystem.DB.TransferDB import TransferDB
from DIRAC.RequestManagementSystem.DB.RequestDBMySQL import RequestDBMySQL
from DIRAC.Resources.Storage.StorageFactory import StorageFactory
from DIRAC.RequestManagementSystem.Client.RequestContainer import RequestContainer
from DIRAC.RequestManagementSystem.DB.RequestDBMySQL import RequestDBMySQL 

AGENT_NAME = "DataManagement/FTSCleaningAgent"

def loadPlugin( pluginPath ):
  """ Create an instance of requested plugin class, loading and importing it when needed. 
  
  This function could raise ImportError when plugin cannot be find or TypeError when 
  loaded class object isn't inherited from FTSCurePlugin class. 
   
  :param str pluginName: dotted path to plugin, specified as in import statement, i.e.
  "DIRAC.CheesShopSystem.private.Cheddar" or alternatively in 'normal' path format 
  "DIRAC/CheesShopSystem/private/Cheddar"

  :return: object instance 
  
  This function try to load and instantiate an object from given path. It is assumed that:

  - :pluginPath: is pointing to module directory "importable" by python interpreter, i.e.: it's 
    package's top level directory is in $PYTHONPATH env variable,
  - the module should consist a class definition following module name,
  - the class itself is inherited from DIRAC.DataManagementSystem.private.FTSCurePlugin.FTSCurePlugin 
 
  If above conditions aren't meet, function is throwing exceptions:

  - ImportError when class cannot be imported
  - TypeError when class isn't inherited from FTSCurePlugin

  
                         
  """

  if "/" in pluginPath:
    pluginPath = ".".join( [ chunk for chunk in pluginPath.split("/") if chunk ] ) 

  pluginName = pluginPath.split(".")[-1]

  if pluginName not in globals():
    mod = __import__( pluginPath, globals(), fromlist=[ pluginName ] )
    pluginClassObj = getattr( mod, pluginName )
  else:
    pluginClassObj = globals()[pluginName]
      
  if not issubclass( pluginClassObj, FTSCurePlugin ):
    raise TypeError( "requested plugin %s isn't inherited from FTSCurePlugin" % pluginName )
  ## return an instance
  return pluginClassObj()
  
########################################################################
class FTSCleaningAgent( AgentModule, RequestAgentBase ):
  """
  .. class:: FTSCleaningAgent

  quick and dirty fixing of FTS and DMS 
  """
        
  def initialize( self ):
    """ Agent initialization.

    :param self: self reference
    """
    ## dict of plugins to be executed, 
    ## key i s a plugin path, i.e. "DIRAC.CheesShopSystem.private.Cheddar"
    ## value is a plugin instance 
    self.plugins = { }
    ## shifterProxy
    self.am_setOption( "shifterProxy", "DataManager" )
    
  def loadPlugins( self ):
    """ Load plugins defined in config for this agent.

    :param self: self reference
    """
    ## read plugin list form config section
    plugins = self.am_getOption( "Plugins", [ "DIRAC.DataManagementSystem.private.FixSURLEqTURLPlugin" ] )
    ## reformat to 'dotted' import path
    plugins = [ ".".join( [ chunk for chunk in pluginPath.split("/") if chunk ] ) for pluginPath in plugins ]
        
    ## remove old plugins 
    for pluginPath in self.plugins:
      if pluginPath not in plugins:
        del self.plugins[ pluginPath ]
        
    ## add new plugins to plugin dict
    for pluginPath in plugins:
      if pluginPath not in self.plugins:
        try:
          self.plugins[ pluginPath ] = loadPlugin( pluginPath )
        except ( ImportError, TypeError ), error:
          self.log.exception( error )
          return S_ERROR( str(error) )

    return S_OK()

  def execute( self ):
    """ Execute plugins in separate threads.

    :param self: self reference
    
    one thread for each plugin 

    """    
    ## load new plugins
    reloadPlugin = self.loadPlugins()
    if not reloadPlugin["OK"]:
      self.log.error("Unable to (re)load reuqested plugins: %s" % reloadPlugin["Message"] )
      return reloadPlugin

    ## check for available plugins
    if not self.plugins:
      self.log.warn( "No plugins to execute found in this cycle.")  
      return S_OK()
    
    ## build thread pool, execute each plugin in his own thread
    self.threadPool = ThreadPool( 1, len(self.plugins) )
    for pluginName, pluginInstance in self.plugins:
      self.threadPool.queueJob( ThreadedJob( pluginInstance.execute ) )
    self.threadPool.processResults()
    return S_OK()
