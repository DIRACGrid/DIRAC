# $HeadURL:  $
''' CacheFeederAgent

  This agent feeds the Cache tables with the outputs of the cache commands.

'''

#from datetime import datetime

from DIRAC                                                      import S_OK, S_ERROR, gConfig
from DIRAC.AccountingSystem.Client.ReportsClient                import ReportsClient
from DIRAC.Core.Base.AgentModule                                import AgentModule
from DIRAC.Core.DISET.RPCClient                                 import RPCClient
from DIRAC.Core.LCG.GOCDBClient                                 import GOCDBClient
from DIRAC.ResourceStatusSystem.Client.JobsClient               import JobsClient
from DIRAC.ResourceStatusSystem.Client.PilotsClient             import PilotsClient
from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient     import ResourceStatusClient
from DIRAC.ResourceStatusSystem.Command.CommandCaller           import CommandCaller
from DIRAC.ResourceStatusSystem.Utilities                       import CSHelpers

__RCSID__  = '$Id:  $'
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

    self.rmClient = ResourceManagementClient()
    
    self.commands = {}
    
    #ClientsCacheCommand
    self.commands[ 'ClientsCache' ] = {
                                        'JobsEffSimpleEveryOne'     : {},
                                        'PilotsEffSimpleEverySites' : {},
                                        'DTEverySites'              : {},
                                        'DTEveryResources'          : {}
                                       }
    #AccountingCacheCommand
    self.commands[ 'AccountingCache' ] = {
                                          'TransferQualityByDestSplitted'     : { 'hours' : 2 },
                                          'FailedTransfersBySourceSplitted'   : { 'hours' : 2 },
                                          'TransferQualityByDestSplittedSite' : { 'hours' : 24 },
                                          'SuccessfullJobsBySiteSplitted'     : { 'hours' : 24 },
                                          'FailedJobsBySiteSplitted'          : { 'hours' : 24 },
                                          'SuccessfullPilotsBySiteSplitted'   : { 'hours' : 24 },
                                          'FailedPilotsBySiteSplitted'        : { 'hours' : 24 },
                                          'SuccessfullPilotsByCESplitted'     : { 'hours' : 24 },
                                          'FailedPilotsByCESplitted'          : { 'hours' : 24 },
                                          'RunningJobsBySiteSplitted'         : { 'hours' : 24 },
                                          'RunningJobsBySiteSplitted'         : { 'hours' : 168 },
                                          'RunningJobsBySiteSplitted'         : { 'hours' : 720 },
                                          'RunningJobsBySiteSplitted'         : { 'hours' : 8760 },    
                                          }
    #VOBOXAvailability
    self.commands[ 'VOBOXAvailability' ] = {
                                            'VOBOXAvailability' : {}
                                            }
    #SpaceTokenOccupancy
    self.commands[ 'SpaceTokenOccupancy' ] = {
                                              'SpaceTokenOccupancy' : {}
                                              }

    #Reuse clients for the commands
    self.clients = {}
    self.clients[ 'GOCDBClient' ]          = GOCDBClient()
    self.clients[ 'JobsClient' ]           = JobsClient()
    self.clients[ 'PilotsClient' ]         = PilotsClient()
    self.clients[ 'ReportGenerator' ]      = RPCClient( 'Accounting/ReportGenerator' )
    self.clients[ 'ReportsClient' ]        = ReportsClient()
    self.clients[ 'ResourceStatusClient' ] = ResourceStatusClient()
    self.clients[ 'WMSAdministrator' ]     = RPCClient( 'WorkloadManagement/WMSAdministrator' )

    cc = CommandCaller()
    
    for commandModule, commandValues in self.commands.items():
      
      self.log.info( '%s module initialization' % commandModule )
      
      for commandName, commandArgs in commandValues.items():

        commandTuple  = ( '%sCommand' % commandModule, '%sCommand' % commandName )
        commandObject = cc.commandInvocation( commandTuple, pArgs = commandArgs,
                                              clients = self.clients )
        
        if not commandObject[ 'OK' ]:
          self.log.error( 'Error initializing %s' % commandName )
          return commandObject
        commandObject = commandObject[ 'Value' ]
        
        self.commands[ commandModule ][ commandName ][ 'command' ] = commandObject
        
        self.log.info( '%s loaded' % commandName )

    return S_OK()

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
    return S_OK( [ { 'serviceURL' : el } for el in elementsToCheck ] )
  
  def __getSpaceTokenOccupancyCandidates( self ):
    '''
    Gets the candidates to execute the command
    '''   
          
    spaceEndpoints = CSHelpers.getSpaceTokenEndpoints()
    if not spaceEndpoints[ 'OK' ]:
      return spaceEndpoints
    spaceEndpoints = spaceEndpoints[ 'Value' ]
    
    spaceTokens = CSHelpers.getSpaceTokens() 
    if not spaceTokens[ 'OK' ]:
      return spaceTokens
    spaceTokens = spaceTokens[ 'Value' ]

    elementsToCheck = []

    for siteDict in spaceEndpoints.values():
      
      if not isinstance( siteDict, dict ):
        continue
      if not siteDict.has_key( 'Endpoint' ):
        continue
      
      for spaceToken in spaceTokens:

        elementsToCheck.append( { 'spaceTokenEndpoint' : siteDict[ 'Endpoint' ],
                                  'spaceToken'         : spaceToken } )
    
    return S_OK( elementsToCheck )
  
  def __logVOBOXAvailabilityResults( self, results ):
    
    if not 'serviceUpTime' in results:
      return S_ERROR( 'serviceUpTime key missing' )
    if not 'machineUpTime' in results:
      return S_ERROR( 'machineUpTime key missing' )
    if not 'site' in results:
      return S_ERROR( 'site key missing' )
    if not 'system' in results:
      return S_ERROR( 'system key missing' )
    
    serviceUp = results[ 'serviceUpTime' ]
    machineUp = results[ 'machineUpTime' ]
    site      = results[ 'site' ]
    system    = results[ 'system' ]
       
    return self.rmClient.addOrModifyVOBOXCache( site, system, serviceUp, machineUp ) 
      
  def execute( self ):        
      
    for commandModule, commandValues in self.commands.items():
      
      for commandName, commandArgs in commandValues.items():
      
        extraArgs = self.getExtraArgs( commandName )  
        if not extraArgs[ 'OK' ]:
          self.log.error( extraArgs[ 'Message' ] )
          return extraArgs  
        extraArgs = extraArgs[ 'Value' ]
          
        for extraArg in extraArgs:  
                    
          commandObject = commandArgs[ 'command' ]
          commandObject.args.update( extraArg )
          
          results = commandObject.doCommand()
          
          self.log.info( '%s/%s with %s' % ( commandModule, commandName, commandObject.args ) )
          
          if not results[ 'OK' ]:
            self.log.error( results[ 'Message' ] )
            continue
          results = results[ 'Value' ]
          
          logResults = self.logResults( commandModule, commandName, results )
          if not logResults[ 'OK' ]:
            self.log.error( logResults[ 'Message' ] )
          
    return S_OK()  
         
  def getExtraArgs( self, commandName ):
    
    extraArgs = S_OK( [ {} ])
    
    if commandName == 'VOBOXAvailability':
      extraArgs = self.__getVOBOXAvailabilityCandidates()
    elif commandName == 'SpaceTokenOccupancy':
      extraArgs = self.__getSpaceTokenOccupancyCandidates()
    
    return extraArgs

  def logResults( self, commandModule, commandName, results ):
       
    if commandModule == 'VOBOXAvailability':
      return self.__logVOBOXAvailabilityResults( results )  
    
    return S_ERROR( 'No log method for %s/%s' % ( commandModule, commandName ) )  
    
      
#  def execute2( self ):
#
#    try:
#
#      now = datetime.utcnow()
#
#      #VOBOX
#      for co in self.commandObjectsVOBOXAvailability:
#        
#        commandName = co[0][1].split( '_' )[0]
#        self.log.info( 'Executed %s with %s' % ( commandName, str( co[2] ) ) )
#
#        co[1].setArgs( co[2] )
#        self.clientsInvoker.setCommand( co[1] )
#        res = self.clientsInvoker.doCommand()[ 'Result' ]
#        
#        if not res[ 'OK' ]:
#          self.log.warn( str( res[ 'Message' ] ) )
#          continue
#
#        res = res[ 'Value' ] 
#
#        serviceUp = res[ 'serviceUpTime' ]
#        machineUp = res[ 'machineUpTime' ]
#        site      = res[ 'site' ]
#        system    = res[ 'system' ]
#       
#        resQuery = self.rmClient.addOrModifyVOBOXCache( site, system, serviceUp, 
#                                                        machineUp, now )    
#        if not resQuery[ 'OK' ]:
#          self.log.error( str( resQuery[ 'Message' ] ) ) 
#
#      #SpaceTokenOccupancy
#      for co in self.commandObjectsSpaceTokenOccupancy:
#        
#        commandName = co[0][1].split( '_' )[0]
#        self.log.info( 'Executed %s with %s' % ( commandName, str( co[2] ) ) )
#
#        co[1].setArgs( co[2] )
#        self.clientsInvoker.setCommand( co[1] )
#        res = self.clientsInvoker.doCommand()[ 'Result' ]
#        
#        if not res[ 'OK' ]:
#          self.log.warn( res[ 'Message' ] )
#          continue
#
#        site, token = co[ 2 ]
#
#        res = res[ 'Value' ]
#        
#        total      = res[ 'total' ]
#        guaranteed = res[ 'guaranteed' ]
#        free       = res[ 'free' ]
#               
#        resQuery = self.rmClient.addOrModifySpaceTokenOccupancyCache( site, token, 
#                                                                      total, guaranteed,
#                                                                      free, now )    
#        if not resQuery[ 'OK' ]:
#          self.log.error( str( resQuery[ 'Message' ] ) )                     
#
#      for co in self.commandObjectsListClientsCache:
#
#        commandName = co[0][1].split( '_' )[0]
#        self.log.info( 'Executed %s' % commandName )
#        try:
#          self.clientsInvoker.setCommand( co[1] )
#          res = self.clientsInvoker.doCommand()['Result']
#
#          if not res['OK']:
#            self.log.warn( res['Message'] )
#            continue
#          res = res[ 'Value' ]
#
#          if not res or res is None:
#            self.log.info('  returned empty...')
#            continue
#          self.log.debug( res )
#
#          for key in res.keys():
#
#            clientCache = ()
#
#            if 'ID' in res[key].keys():
#
#              for value in res[key].keys():
#                if value != 'ID':
#                  clientCache = ( key.split()[1], commandName, res[key]['ID'],
#                                  value, res[key][value], None, None )
#
#                  resQuery = self.rmClient.addOrModifyClientCache( *clientCache )
#                  if not resQuery[ 'OK' ]:
#                    self.log.error( resQuery[ 'Message' ] )
#
#            else:
#              for value in res[key].keys():
#                clientCache = ( key, commandName, None, value,
#                                res[key][value], None, None )
#
#                resQuery = self.rmClient.addOrModifyClientCache( *clientCache )
#                if not resQuery[ 'OK' ]:
#                  self.log.error( resQuery[ 'Message' ] )
#
#        except:
#          self.log.exception( "Exception when executing " + co[0][1] )
#          continue
#
#      now = datetime.utcnow().replace( microsecond = 0 )
#
#      for co in self.commandObjectsListAccountingCache:
#
#        if co[0][3] == 'Hourly':
#          if now.minute >= 10:
#            continue
#        elif co[0][3] == 'Daily':
#          if now.hour >= 1:
#            continue
#
#        commandName = co[0][1].split( '_' )[0]
#        plotName    = commandName + '_' + str( co[2][0] )
#
#        self.log.info( 'Executed %s with args %s %s' % ( commandName, co[0][2], co[0][3] ) )
#
#        try:
#          co[1].setArgs( co[2] )
#          self.clientsInvoker.setCommand( co[1] )
#          res = self.clientsInvoker.doCommand()['Result']
#
#          if not res['OK']:
#            self.log.warn( res['Message'] )
#            continue
#          res = res[ 'Value' ]
#
#          if not res or res is None:
#            self.log.info('  returned empty...')
#            continue
#          self.log.debug( res )
#
#          plotType = res.keys()[ 0 ]
#
#          if not res[ plotType ]:
#            self.log.info('  returned empty...')
#          self.log.debug( res )
#
#          for name in res[ plotType ].keys():
#
#            #name, plotType, plotName, result, dateEffective, lastCheckTime
#            accountingClient = ( name, plotType, plotName, str(res[plotType][name]), None, None )
#            resQuery = self.rmClient.addOrModifyAccountingCache( *accountingClient )
#            if not resQuery[ 'OK' ]:
#              self.log.error( resQuery[ 'Message' ] )
#
#        except:
#          self.log.exception( "Exception when executing " + commandName )
#          continue
#
#      return S_OK()
#
#    except Exception:
#      errorStr = "CacheFeederAgent execution"
#      self.log.exception( errorStr )
#      return S_ERROR( errorStr )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF