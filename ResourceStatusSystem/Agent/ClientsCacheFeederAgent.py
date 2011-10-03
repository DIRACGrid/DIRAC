################################################################################
# $HeadURL:  $
################################################################################
""" This agents feeds the ClientsCache table.
"""

import datetime

from DIRAC                                              import S_OK, S_ERROR
from DIRAC                                              import gLogger

from DIRAC.Core.Base.AgentModule                        import AgentModule
from DIRAC.Core.DISET.RPCClient                         import RPCClient
#from DIRAC.ResourceStatusSystem.DB.ResourceStatusDB     import ResourceStatusDB
from DIRAC.ResourceStatusSystem.DB.ResourceManagementDB import ResourceManagementDB
from DIRAC.ResourceStatusSystem.Command.CommandCaller   import CommandCaller
from DIRAC.ResourceStatusSystem.Command.ClientsInvoker  import ClientsInvoker

__RCSID__ = "$Id:  $"

AGENT_NAME = 'ResourceStatus/ClientsCacheFeederAgent'

class ClientsCacheFeederAgent( AgentModule ):

################################################################################

  def initialize( self ):
    """ ClientsCacheFeederAgent initialization
    """

    try:

      #self.rsDB = ResourceStatusDB()
      self.rmDB = ResourceManagementDB()

      self.clientsInvoker = ClientsInvoker()

      commandsList_ClientsCache = [
        ( 'ClientsCache_Command', 'JobsEffSimpleEveryOne_Command' ),
        ( 'ClientsCache_Command', 'PilotsEffSimpleEverySites_Command' ),
        ( 'ClientsCache_Command', 'DTEverySites_Command' ),
        ( 'ClientsCache_Command', 'DTEveryResources_Command' )
        ]

      commandsList_AccountingCache =  [
        ( 'AccountingCache_Command', 'TransferQualityByDestSplitted_Command', ( 2, ), 'Always' ),
        ( 'AccountingCache_Command', 'FailedTransfersBySourceSplitted_Command', ( 2, ), 'Always' ),
        ( 'AccountingCache_Command', 'TransferQualityByDestSplittedSite_Command', ( 24, ), 'Hourly' ),
        ( 'AccountingCache_Command', 'SuccessfullJobsBySiteSplitted_Command', ( 24, ), 'Hourly' ),
        ( 'AccountingCache_Command', 'FailedJobsBySiteSplitted_Command', ( 24, ), 'Hourly' ),
        ( 'AccountingCache_Command', 'SuccessfullPilotsBySiteSplitted_Command', ( 24, ), 'Hourly' ),
        ( 'AccountingCache_Command', 'FailedPilotsBySiteSplitted_Command', ( 24, ), 'Hourly' ),
        ( 'AccountingCache_Command', 'SuccessfullPilotsByCESplitted_Command', ( 24, ), 'Hourly' ),
        ( 'AccountingCache_Command', 'FailedPilotsByCESplitted_Command', ( 24, ), 'Hourly' ),
        ( 'AccountingCache_Command', 'RunningJobsBySiteSplitted_Command', ( 24, ), 'Hourly' ),
        ( 'AccountingCache_Command', 'RunningJobsBySiteSplitted_Command', ( 168, ), 'Hourly' ),
        ( 'AccountingCache_Command', 'RunningJobsBySiteSplitted_Command', ( 720, ), 'Daily' ),
        ( 'AccountingCache_Command', 'RunningJobsBySiteSplitted_Command', ( 8760, ), 'Daily' ),
        ]

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

################################################################################

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
        print co  
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
          print res
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

################################################################################

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF