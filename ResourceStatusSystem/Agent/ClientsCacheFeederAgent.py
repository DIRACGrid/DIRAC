########################################################################
# $HeadURL:  $
########################################################################
""" This agents feeds the ClientsCache table.
"""

import copy, datetime

from DIRAC import S_OK, S_ERROR
from DIRAC import gLogger

from DIRAC.ResourceStatusSystem.Utilities.CS import getExt

from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.ResourceStatusSystem.DB.ResourceStatusDB import ResourceStatusDB
from DIRAC.ResourceStatusSystem.DB.ResourceManagementDB import ResourceManagementDB
from DIRAC.ResourceStatusSystem.Command.CommandCaller import CommandCaller
from DIRAC.ResourceStatusSystem.Command.ClientsInvoker import ClientsInvoker

__RCSID__ = "$Id:  $"

AGENT_NAME = 'ResourceStatus/ClientsCacheFeederAgent'

class ClientsCacheFeederAgent( AgentModule ):

#############################################################################

  def initialize( self ):
    """ ClientsCacheFeederAgent initialization
    """

    try:

      self.rsDB = ResourceStatusDB()
      self.rmDB = ResourceManagementDB()

      self.clientsInvoker = ClientsInvoker()

      VOExtension = getExt()

      module = "DIRAC.ResourceStatusSystem.Policy.Configurations"

#      try:
#          
#        configModule = __import__( VOExtension + "DIRAC.ResourceStatusSystem.Policy.Configurations",
#                                   globals(), locals(), ['*'] )
#      except:
      configModule = __import__( module, globals(), locals(), ['*'] )
            
      commandsList_ClientsCache = copy.deepcopy( configModule.Commands_ClientsCache )

      commandsList_AccountingCache = copy.deepcopy( configModule.Commands_AccountingCache )

      self.commandObjectsList_ClientsCache = []

      self.commandObjectsList_AccountingCache = []

      cc = CommandCaller()

      RPCWMSAdmin = RPCClient( "WorkloadManagement/WMSAdministrator" )
      RPCAccounting = RPCClient( "Accounting/ReportGenerator" )

      for command in commandsList_ClientsCache:
          
        cObj = cc.setCommandObject( command )
        cc.setCommandClient( command, cObj, RPCWMSAdmin = RPCWMSAdmin,
                            RPCAccounting = RPCAccounting )
        self.commandObjectsList_ClientsCache.append( ( command, cObj ) )

      for command in commandsList_AccountingCache:
        cObj = cc.setCommandObject( command )
        cc.setCommandClient( command, cObj, RPCAccounting = RPCAccounting )
        try:
          cArgs = command[2]
        except IndexError:
          cArgs = ()
        self.commandObjectsList_AccountingCache.append( ( command, cObj, cArgs ) )

      return S_OK()

    except Exception:
      errorStr = "ClientsCacheFeederAgent initialization"
      gLogger.exception( errorStr )
      return S_ERROR( errorStr )

#############################################################################

  def execute( self ):
    """ The main ClientsCacheFeederAgent execution method
    """

    try:

      now = datetime.datetime.utcnow()

      for co in self.commandObjectsList_ClientsCache:
        try:
          self.clientsInvoker.setCommand( co[1] )
          res = self.clientsInvoker.doCommand()
          for key in res.keys():
            if 'ID' in res[key].keys():
              for value in res[key].keys():
                if value != 'ID':
                  self.rmDB.addOrModifyClientsCacheRes( key.split()[1], co[0][1].split( '_' )[0],
                                                       value, res[key][value], res[key]['ID'] )
            else:
              for value in res[key].keys():
                self.rmDB.addOrModifyClientsCacheRes( key, co[0][1].split( '_' )[0],
                                                     value, res[key][value] )
        except:
          gLogger.exception( "Exception when executing " + co[0][1] )
          continue

      for co in self.commandObjectsList_AccountingCache:
        if co[0][3] == 'Hourly':
          if now.minute >= 10:
            continue
        elif co[0][3] == 'Daily':
          if now.hour >= 1:
            continue

        try:
          co[1].setArgs( co[2] )
          self.clientsInvoker.setCommand( co[1] )
          res = self.clientsInvoker.doCommand()
          plotType = res.keys()[ 0 ]
          for name in res[ plotType ].keys():
            plotName = co[0][1].split( '_' )[0] + '_' + str( co[2][0] )
            self.rmDB.addOrModifyAccountingCacheRes( name, plotType, plotName,
                                                    res[plotType][name] )
        except:
          gLogger.exception( "Exception when executing " + co[ 0 ][ 1 ] )
          continue

      return S_OK()

    except Exception:
      errorStr = "ClientsCacheFeederAgent execution"
      gLogger.exception( errorStr )
      return S_ERROR( errorStr )

#############################################################################
