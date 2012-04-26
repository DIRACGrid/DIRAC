# $HeadURL:  $
''' CacheFeederAgent

  This agent feeds the Cache tables with the outputs of the cache commands.

'''

from datetime import datetime

from DIRAC                                                      import S_OK, S_ERROR, gConfig
from DIRAC.Core.Base.AgentModule                                import AgentModule
from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient
from DIRAC.ResourceStatusSystem.Command.CommandCaller           import CommandCaller
from DIRAC.ResourceStatusSystem.Command.ClientsInvoker          import ClientsInvoker
from DIRAC.ResourceStatusSystem.Command.knownAPIs               import initAPIs
from DIRAC.ResourceStatusSystem.Utilities                       import CS

__RCSID__  = '$Id: $'
AGENT_NAME = 'ResourceStatus/CacheFeederAgent'

class CacheFeederAgent( AgentModule ):
  '''
  The CacheFeederAgent feeds the cache tables for the client and the accounting.
  It runs periodically a set of commands, and stores it's results on the
  tables.
  '''

  # Too many public methods
  # pylint: disable-msg=R0904  

  def initialize( self ):

    # Attribute defined outside __init__ 
    # pylint: disable-msg=W0201
    
    try:

      self.rmClient       = ResourceManagementClient()
      self.clientsInvoker = ClientsInvoker()

      commandsListClientsCache = [
        ( 'ClientsCache_Command', 'JobsEffSimpleEveryOne_Command'     ),
        ( 'ClientsCache_Command', 'PilotsEffSimpleEverySites_Command' ),
        ( 'ClientsCache_Command', 'DTEverySites_Command'              ),
        ( 'ClientsCache_Command', 'DTEveryResources_Command'          )
        ]

      commandsListAccountingCache =  [
        ( 'AccountingCache_Command', 'TransferQualityByDestSplitted_Command',     ( 2, ),    'Always' ),
        ( 'AccountingCache_Command', 'FailedTransfersBySourceSplitted_Command',   ( 2, ),    'Always' ),
        ( 'AccountingCache_Command', 'TransferQualityByDestSplittedSite_Command', ( 24, ),   'Hourly' ),
        ( 'AccountingCache_Command', 'SuccessfullJobsBySiteSplitted_Command',     ( 24, ),   'Hourly' ),
        ( 'AccountingCache_Command', 'FailedJobsBySiteSplitted_Command',          ( 24, ),   'Hourly' ),
        ( 'AccountingCache_Command', 'SuccessfullPilotsBySiteSplitted_Command',   ( 24, ),   'Hourly' ),
        ( 'AccountingCache_Command', 'FailedPilotsBySiteSplitted_Command',        ( 24, ),   'Hourly' ),
        ( 'AccountingCache_Command', 'SuccessfullPilotsByCESplitted_Command' ,    ( 24, ),   'Hourly' ),
        ( 'AccountingCache_Command', 'FailedPilotsByCESplitted_Command',          ( 24, ),   'Hourly' ),
        ( 'AccountingCache_Command', 'RunningJobsBySiteSplitted_Command',         ( 24, ),   'Hourly' ),
        ( 'AccountingCache_Command', 'RunningJobsBySiteSplitted_Command',         ( 168, ),  'Hourly' ),
        ( 'AccountingCache_Command', 'RunningJobsBySiteSplitted_Command',         ( 720, ),  'Daily'  ),
        ( 'AccountingCache_Command', 'RunningJobsBySiteSplitted_Command',         ( 8760, ), 'Daily'  ),
        ]

      commandsVOBOXAvailability   = ( 'VOBOXAvailabilityCommand', 'VOBOXAvailabilityCommand', )
      commandsSpaceTokenOccupancy = ( 'SpaceTokenOccupancyCommand', 'SpaceTokenOccupancyCommand', )

      self.commandObjectsListClientsCache     = []
      self.commandObjectsListAccountingCache  = []
      self.commandObjectsVOBOXAvailability    = [] 
      self.commandObjectsSpaceTokenOccupancy = []


      cc = CommandCaller()

      # We know beforehand which APIs are we going to need, so we initialize them
      # first, making everything faster.
      knownAPIs = [ 'ResourceStatusClient', 'WMSAdministrator', 'ReportGenerator',
                    'JobsClient', 'PilotsClient', 'GOCDBClient', 'ReportsClient' ]
      knownAPIs = initAPIs( knownAPIs, {} )

      for command in commandsListClientsCache:

        cObj = cc.setCommandObject( command )
        for apiName, apiInstance in knownAPIs.items():
          cc.setAPI( cObj, apiName, apiInstance )

        self.commandObjectsListClientsCache.append( ( command, cObj ) )

      for command in commandsListAccountingCache:

        cObj = cc.setCommandObject( command )
        for apiName, apiInstance in knownAPIs.items():
          cc.setAPI( cObj, apiName, apiInstance )
        cArgs = command[ 2 ]

        self.commandObjectsListAccountingCache.append( ( command, cObj, cArgs ) )

      for cArgs in self.__getVOBOXAvailabilityCandidates():
        
        cObj  = cc.setCommandObject( commandsVOBOXAvailability )
        self.commandObjectsVOBOXAvailability.append( ( commandsVOBOXAvailability, cObj, cArgs ) )

      for cArgs in self.__getSpaceTokenOccupancyCandidates():
        
        cObj  = cc.setCommandObject( commandsSpaceTokenOccupancy )
        self.commandObjectsSpaceTokenOccupancy.append( ( commandsSpaceTokenOccupancy, cObj, cArgs ) )

      return S_OK()

    except Exception:
      errorStr = "CacheFeederAgent initialization"
      self.log.exception( errorStr )
      return S_ERROR( errorStr )

################################################################################

  def __getVOBOXAvailabilityCandidates( self ):
    '''
    Gets the candidates to execute the command
    '''
    
    # This is horrible, future me, change this.
    request_management_urls = gConfig.getValue( '/Systems/RequestManagement/Development/URLs/allURLS', [] )
    configuration_urls      = gConfig.getServersList()
    framework_urls          = gConfig.getValue( '/DIRAC/Framework/SystemAdministrator', [] )
    
    elementsToCheck = request_management_urls + configuration_urls + framework_urls 
  
    # This may look stupid, but the Command is expecting a tuple
    return [ ( el, ) for el in elementsToCheck ] 
  
  def __getSpaceTokenOccupancyCandidates( self ):
    '''
    Gets the candidates to execute the command
    '''
    
    elementsToCheck = []      
    spaceEndpoints  = CS.getSpaceTokenEndpoints()
    spaceTokens     = CS.getSpaceTokens() 

    for site,siteDict in spaceEndpoints.items():
      
      if not isinstance( siteDict, dict ):
        continue
      if not siteDict.has_key( 'Endpoint' ):
        continue
      
      for spaceToken in spaceTokens:

        elementsToCheck.append( ( siteDict[ 'Endpoint' ], spaceToken, ) )
    
    return elementsToCheck
      
  def execute( self ):

    try:

      now = datetime.utcnow()

      #VOBOX
      for co in self.commandObjectsVOBOXAvailability:
        
        commandName = co[0][1].split( '_' )[0]
        self.log.info( 'Executed %s with %s' % ( commandName, str( co[2] ) ) )

        co[1].setArgs( co[2] )
        self.clientsInvoker.setCommand( co[1] )
        res = self.clientsInvoker.doCommand()[ 'Result' ]
        
        if not res[ 'OK' ]:
          self.log.warn( str( res[ 'Message' ] ) )
          continue

        res = res[ 'Value' ] 

        serviceUp = res[ 'serviceUpTime' ]
        machineUp = res[ 'machineUpTime' ]
        site      = res[ 'site' ]
        system    = res[ 'system' ]
       
        resQuery = self.rmClient.addOrModifyVOBOXCache( site, system, serviceUp, 
                                                        machineUp, now )    
        if not resQuery[ 'OK' ]:
          self.log.error( str( resQuery[ 'Message' ] ) ) 

      #SpaceTokenOccupancy
      for co in self.commandObjectsSpaceTokenOccupancy:
        
        commandName = co[0][1].split( '_' )[0]
        self.log.info( 'Executed %s with %s' % ( commandName, str( co[2] ) ) )

        co[1].setArgs( co[2] )
        self.clientsInvoker.setCommand( co[1] )
        res = self.clientsInvoker.doCommand()[ 'Result' ]
        
        if not res[ 'OK' ]:
          self.log.warn( res[ 'Message' ] )
          continue

        site, token = co[ 2 ]

        res = res[ 'Value' ]
        
        total      = res[ 'total' ]
        guaranteed = res[ 'guaranteed' ]
        free       = res[ 'free' ]
               
        resQuery = self.rmClient.addOrModifySpaceTokenOccupancyCache( site, token, 
                                                                      total, guaranteed,
                                                                      free, now )    
        if not resQuery[ 'OK' ]:
          self.log.error( str( resQuery[ 'Message' ] ) )                     

      for co in self.commandObjectsListClientsCache:

        commandName = co[0][1].split( '_' )[0]
        self.log.info( 'Executed %s' % commandName )
        try:
          self.clientsInvoker.setCommand( co[1] )
          res = self.clientsInvoker.doCommand()['Result']

          if not res['OK']:
            self.log.warn( res['Message'] )
            continue
          res = res[ 'Value' ]

          if not res or res is None:
            self.log.info('  returned empty...')
            continue
          self.log.debug( res )

          for key in res.keys():

            clientCache = ()

            if 'ID' in res[key].keys():

              for value in res[key].keys():
                if value != 'ID':
                  clientCache = ( key.split()[1], commandName, res[key]['ID'],
                                  value, res[key][value], None, None )

                  resQuery = self.rmClient.addOrModifyClientCache( *clientCache )
                  if not resQuery[ 'OK' ]:
                    self.log.error( resQuery[ 'Message' ] )

            else:
              for value in res[key].keys():
                clientCache = ( key, commandName, None, value,
                                res[key][value], None, None )

                resQuery = self.rmClient.addOrModifyClientCache( *clientCache )
                if not resQuery[ 'OK' ]:
                  self.log.error( resQuery[ 'Message' ] )

        except:
          self.log.exception( "Exception when executing " + co[0][1] )
          continue

      now = datetime.utcnow().replace( microsecond = 0 )

      for co in self.commandObjectsListAccountingCache:

        if co[0][3] == 'Hourly':
          if now.minute >= 10:
            continue
        elif co[0][3] == 'Daily':
          if now.hour >= 1:
            continue

        commandName = co[0][1].split( '_' )[0]
        plotName    = commandName + '_' + str( co[2][0] )

        self.log.info( 'Executed %s with args %s %s' % ( commandName, co[0][2], co[0][3] ) )

        try:
          co[1].setArgs( co[2] )
          self.clientsInvoker.setCommand( co[1] )
          res = self.clientsInvoker.doCommand()['Result']

          if not res['OK']:
            self.log.warn( res['Message'] )
            continue
          res = res[ 'Value' ]

          if not res or res is None:
            self.log.info('  returned empty...')
            continue
          self.log.debug( res )

          plotType = res.keys()[ 0 ]

          if not res[ plotType ]:
            self.log.info('  returned empty...')
          self.log.debug( res )

          for name in res[ plotType ].keys():

            #name, plotType, plotName, result, dateEffective, lastCheckTime
            accountingClient = ( name, plotType, plotName, str(res[plotType][name]), None, None )
            resQuery = self.rmClient.addOrModifyAccountingCache( *accountingClient )
            if not resQuery[ 'OK' ]:
              self.log.error( resQuery[ 'Message' ] )

        except:
          self.log.exception( "Exception when executing " + commandName )
          continue

      return S_OK()

    except Exception:
      errorStr = "CacheFeederAgent execution"
      self.log.exception( errorStr )
      return S_ERROR( errorStr )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF