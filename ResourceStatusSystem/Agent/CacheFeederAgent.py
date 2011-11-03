################################################################################
# $HeadURL:  $
################################################################################
__RCSID__  = "$Id:  $"
AGENT_NAME = 'ResourceStatus/CacheFeederAgent'

from datetime import datetime

from DIRAC                                                import S_OK, S_ERROR
from DIRAC                                                import gLogger
from DIRAC.Core.Base.AgentModule                          import AgentModule

from DIRAC.ResourceStatusSystem.API.ResourceManagementAPI import ResourceManagementAPI
from DIRAC.ResourceStatusSystem.Command.CommandCaller     import CommandCaller
from DIRAC.ResourceStatusSystem.Command.ClientsInvoker    import ClientsInvoker
from DIRAC.ResourceStatusSystem.Command.knownAPIs         import initAPIs

class CacheFeederAgent( AgentModule ):
  '''
  The CacheFeederAgent feeds the cache tables for the client and the accounting.
  It runs periodically a set of commands, and stores it's results on the
  tables.  
  '''

  def initialize( self ):

    try:

      self.rmAPI          = ResourceManagementAPI()
      self.clientsInvoker = ClientsInvoker()   
         
      commandsList_ClientsCache = [
        ( 'ClientsCache_Command', 'JobsEffSimpleEveryOne_Command'     ),
        ( 'ClientsCache_Command', 'PilotsEffSimpleEverySites_Command' ),
        ( 'ClientsCache_Command', 'DTEverySites_Command'              ),
        ( 'ClientsCache_Command', 'DTEveryResources_Command'          )
        ]

      commandsList_AccountingCache =  [
        ( 'AccountingCache_Command', 'TransferQualityByDestSplitted_Command',     ( 2, ),    'Always' ),
        ( 'AccountingCache_Command', 'FailedTransfersBySourceSplitted_Command',   ( 2, ),    'Always' ),
        ( 'AccountingCache_Command', 'TransferQualityByDestSplittedSite_Command', ( 24, ),   'Hourly' ),
        ( 'AccountingCache_Command', 'SuccessfullJobsBySiteSplitted_Command',     ( 24, ),   'Hourly' ),
        ( 'AccountingCache_Command', 'FailedJobsBySiteSplitted_Command',          ( 24, ),   'Hourly' ),
        ( 'AccountingCache_Command', 'SuccessfullPilotsBySiteSplitted_Command',   ( 24, ),   'Hourly' ),
        ( 'AccountingCache_Command', 'FailedPilotsBySiteSplitted_Command',        ( 24, ),   'Hourly' ),
        ( 'AccountingCache_Command', 'SuccessfullPilotsByCESplitted_Command',     ( 24, ),   'Hourly' ),
        ( 'AccountingCache_Command', 'FailedPilotsByCESplitted_Command',          ( 24, ),   'Hourly' ),
        ( 'AccountingCache_Command', 'RunningJobsBySiteSplitted_Command',         ( 24, ),   'Hourly' ),
        ( 'AccountingCache_Command', 'RunningJobsBySiteSplitted_Command',         ( 168, ),  'Hourly' ),
        ( 'AccountingCache_Command', 'RunningJobsBySiteSplitted_Command',         ( 720, ),  'Daily'  ),
        ( 'AccountingCache_Command', 'RunningJobsBySiteSplitted_Command',         ( 8760, ), 'Daily'  ),
        ]

      self.commandObjectsList_ClientsCache    = []
      self.commandObjectsList_AccountingCache = []

      cc = CommandCaller()
      
      # We know beforehand which APIs are we going to need, so we initialize them
      # first, making everything faster.
      APIs = [ 'ResourceStatusAPI', 'WMSAdministrator', 'ReportGenerator',
               'JobsClient', 'PilotsClient', 'GOCDBClient', 'ReportsClient' ]
      APIs = initAPIs( APIs, {} )
      
      for command in commandsList_ClientsCache:

        cObj = cc.setCommandObject( command )
        for apiName, apiInstance in APIs.items():
          cc.setAPI( cObj, apiName, apiInstance )
        
        self.commandObjectsList_ClientsCache.append( ( command, cObj ) )

      for command in commandsList_AccountingCache:
        
        cObj = cc.setCommandObject( command )
        for apiName, apiInstance in APIs.items():
          cc.setAPI( cObj, apiName, apiInstance )  
        cArgs = command[ 2 ]
        
        self.commandObjectsList_AccountingCache.append( ( command, cObj, cArgs ) )

      return S_OK()

    except Exception:
      errorStr = "CacheFeederAgent initialization"
      gLogger.exception( errorStr )
      return S_ERROR( errorStr )

################################################################################

  def execute( self ):

    try:

      for co in self.commandObjectsList_ClientsCache:
        
        commandName = co[0][1].split( '_' )[0]
        gLogger.info( 'Executed %s' % commandName )
        try:
          self.clientsInvoker.setCommand( co[1] )
          res = self.clientsInvoker.doCommand()
          
          if not res or res is None:
            gLogger.info('  returned empty...')
            continue
          gLogger.info( res )
          
          for key in res.keys():
          
            clientCache = ()
          
            if 'ID' in res[key].keys():
              
              for value in res[key].keys():
                if value != 'ID':
                  clientCache = ( key.split()[1], commandName, res[key]['ID'],
                                  value, res[key][value], None, None )
                  
                  resQuery = self.rmAPI.addOrModifyClientCache( *clientCache )
                  if not resQuery[ 'OK' ]:
                    gLogger.error( resQuery[ 'Message' ] )
            
            else:
              for value in res[key].keys():
                clientCache = ( key, commandName, None, value, 
                                res[key][value], None, None )
                    
                    
                resQuery = self.rmAPI.addOrModifyClientCache( *clientCache )
                if not resQuery[ 'OK' ]:
                  gLogger.error( resQuery[ 'Message' ] )        
                
        except:
          gLogger.exception( "Exception when executing " + co[0][1] )
          continue

      now = datetime.utcnow().replace( microsecond = 0 )

      for co in self.commandObjectsList_AccountingCache: 

        if co[0][3] == 'Hourly':
          if now.minute >= 10:
            continue
        elif co[0][3] == 'Daily':
          if now.hour >= 1:
            continue

        commandName = co[0][1].split( '_' )[0]
        plotName    = commandName + '_' + str( co[2][0] )
        
        gLogger.info( 'Executed %s with args %s %s' % ( commandName, co[0][2], co[0][3] ) )

        try:
          co[1].setArgs( co[2] )
          self.clientsInvoker.setCommand( co[1] )
          res = self.clientsInvoker.doCommand()
          
          if not res or res is None:
            gLogger.info('  returned empty...')
            continue
          gLogger.info( res )
          
          plotType = res.keys()[ 0 ]
          
          if not res[ plotType ]:
            gLogger.info('  returned empty...')
          gLogger.debug( res )  
          
          for name in res[ plotType ].keys():
            
            #name, plotType, plotName, result, dateEffective, lastCheckTime
            accountingClient = ( name, plotType, plotName, str(res[plotType][name]), None, None )
            resQuery = self.rmAPI.addOrModifyAccountingCache( *accountingClient )
            if not resQuery[ 'OK' ]:
              gLogger.error( resQuery[ 'Message' ] )
            
        except:
          gLogger.exception( "Exception when executing " + commandName )
          continue

      return S_OK()

    except Exception:
      errorStr = "CacheFeederAgent execution"
      gLogger.exception( errorStr )
      return S_ERROR( errorStr )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF