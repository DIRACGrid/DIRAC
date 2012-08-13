# $HeadURL:  $
''' CacheFeederAgent

  This agent feeds the Cache tables with the outputs of the cache commands.

'''

from DIRAC                                                      import S_OK, S_ERROR, gConfig
from DIRAC.AccountingSystem.Client.ReportsClient                import ReportsClient
from DIRAC.Core.Base.AgentModule                                import AgentModule
from DIRAC.Core.DISET.RPCClient                                 import RPCClient
from DIRAC.Core.LCG.GOCDBClient                                 import GOCDBClient
from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient     import ResourceStatusClient
from DIRAC.ResourceStatusSystem.Command                         import CommandCaller
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

  def __init__( self, agentName, baseAgentName = False, properties = dict() ):
    
    AgentModule.__init__( self, agentName, baseAgentName, properties )
    
    self.commands = {}
    self.clients  = {}    

  def initialize( self ):

    self.rmClient = ResourceManagementClient()

    #FIXME: missing logger
    #JobsCommand
    self.commands[ 'Jobs' ] = [ { 'JobsWMS' : { 'siteName' : None } }]  
    #FIXME: missing logger
    
    self.commands[ 'GGUSTickets' ] = [ { 'GGUSTicketsMaster' : {} }]
    
    #PilotsCommand
    self.commands[ 'Pilots' ] = [ 
                                 { 'PilotsWMS' : { 'element' : 'Site', 'siteName' : None } },
                                 { 'PilotsWMS' : { 'element' : 'Resource', 'siteName' : None } } 
                                 ]
    #DowntimeCommand
    self.commands[ 'Downtime' ] = [    
                                    { 'DowntimeSites'     : {} },
                                    { 'DowntimeResources' : {} }
                                  ] 
    #FIXME: do not forget about hourly vs Always ...etc                                                                       
    #AccountingCacheCommand
    self.commands[ 'AccountingCache' ] = [
                                          {'SuccessfullJobsBySiteSplitted'    :{'hours' :24, 'plotType' :'Job' }},
                                          {'FailedJobsBySiteSplitted'         :{'hours' :24, 'plotType' :'Job' }},
                                          {'SuccessfullPilotsBySiteSplitted'  :{'hours' :24, 'plotType' :'Pilot' }},
                                          {'FailedPilotsBySiteSplitted'       :{'hours' :24, 'plotType' :'Pilot' }},
                                          {'SuccessfullPilotsByCESplitted'    :{'hours' :24, 'plotType' :'Pilot' }},
                                          {'FailedPilotsByCESplitted'         :{'hours' :24, 'plotType' :'Pilot' }},
                                          {'RunningJobsBySiteSplitted'        :{'hours' :24, 'plotType' :'Job' }},
#                                          {'RunningJobsBySiteSplitted'        :{'hours' :168, 'plotType' :'Job' }},
#                                          {'RunningJobsBySiteSplitted'        :{'hours' :720, 'plotType' :'Job' }},
#                                          {'RunningJobsBySiteSplitted'        :{'hours' :8760, 'plotType' :'Job' }},    
                                          ]

    #Transfer
    self.commands[ 'Transfer' ] = [
                                   { 'TransferQuality' : { 'hours' : 2, 'name' : None, 'direction' : 'Source' } }, 
                                   { 'TransferQuality' : { 'hours' : 2, 'name' : None, 'direction' : 'Destination' } },
                                   { 'TransferFailed'  : { 'hours' : 2, 'name' : None, 'direction' : 'Source' } },
                                   { 'TransferFailed'  : { 'hours' : 2, 'name' : None, 'direction' : 'Destination' } },
                                            ]
    
    #VOBOXAvailability
    self.commands[ 'VOBOXAvailability' ] = [
                                            { 'VOBOXAvailability' : {} }
                                            ]
    #SpaceTokenOccupancy
    self.commands[ 'SpaceTokenOccupancy' ] = [
                                              { 'SpaceTokenOccupancy' : {} }
                                              ]
    
    #Reuse clients for the commands
    self.clients[ 'GOCDBClient' ]          = GOCDBClient()
    self.clients[ 'ReportGenerator' ]      = RPCClient( 'Accounting/ReportGenerator' )
    self.clients[ 'ReportsClient' ]        = ReportsClient()
    self.clients[ 'ResourceStatusClient' ] = ResourceStatusClient()
    self.clients[ 'WMSAdministrator' ]     = RPCClient( 'WorkloadManagement/WMSAdministrator' )

    cc = CommandCaller
    
    for commandModule, commandList in self.commands.items():
      
      self.log.info( '%s module initialization' % commandModule )
      
      #for commandName, commandArgs in commandValues.items():
      for commandDict in commandList:

        commandName = commandDict.keys()[0]
        commandArgs = commandDict[ commandName ]

        commandTuple  = ( '%sCommand' % commandModule, '%sCommand' % commandName )
        commandObject = cc.commandInvocation( commandTuple, pArgs = commandArgs,
                                              clients = self.clients )
        
        if not commandObject[ 'OK' ]:
          self.log.error( 'Error initializing %s' % commandName )
          return commandObject
        commandObject = commandObject[ 'Value' ]
        
        commandArgs[ 'command' ] = commandObject
        
        self.log.info( '%s loaded' % commandName )

    return S_OK()

  def execute( self ):        
      
    for commandModule, commandList in self.commands.items():
      
      for commandDict in commandList:
      
        commandName = commandDict.keys()[0]
        commandArgs = commandDict[ commandName ]
      
        extraArgs = self.getExtraArgs( commandName )  
        if not extraArgs[ 'OK' ]:
          self.log.error( extraArgs[ 'Message' ] )
          return extraArgs  
        extraArgs = extraArgs[ 'Value' ]
          
        for extraArg in extraArgs:  
                    
          commandObject = commandArgs[ 'command' ]
          commandObject.args.update( extraArg )

          self.log.info( '%s/%s with %s' % ( commandModule, commandName, commandObject.args ) )
          
          results = commandObject.doCommand()
                    
          if not results[ 'OK' ]:
            self.log.error( results[ 'Message' ] )
            continue
          results = results[ 'Value' ]

          if not results:
            self.log.info( 'Empty results' )
            continue
          
          logResults = self.logResults( commandModule, commandDict, commandObject, results )
          if not logResults[ 'OK' ]:
            self.log.error( logResults[ 'Message' ] )
          
    return S_OK()  
         
  def getExtraArgs( self, commandName ):
    # FIXME: do it by default on the command
    '''
      Some of the commands require a list of 
    '''
    
    extraArgs = S_OK( [ {} ])
    
    if commandName == 'VOBOXAvailability':
      extraArgs = self.__getVOBOXAvailabilityElems()
    elif commandName == 'SpaceTokenOccupancy':
      extraArgs = self.__getSpaceTokenOccupancyElems()
    
    return extraArgs

  def logResults( self, commandModule, commandDict, commandObject, results ):
    '''
      Lazy method to run the appropiated method to log the results in the DB.
    '''

    if commandModule == 'AccountingCache':
      return self.__logAccountingCacheResults( commandDict, results ) 
       
    if commandModule == 'VOBOXAvailability':
      return self.__logVOBOXAvailabilityResults( results )  

    if commandModule == 'Downtime':
      return self.__logDowntimeResults( commandDict, results )  
    
    if commandModule == 'Jobs':
      return self.__logJobsResults( results )

    if commandModule == 'Pilots':
      return self.__logPilotsResults( results )

    if commandModule == 'Transfer':
      return self.__logTransferResults( commandDict, results )
    
    if commandModule == 'SpaceTokenOccupancy':
      return self.__logSpaceTokenOccupancy( commandDict, commandObject, results )
    
    if commandModule == 'GGUSTickets':
      return self.__logGGUSTickets( results )

    commandName = commandDict.keys()[ 0 ]
    return S_ERROR( 'No log method for %s/%s' % ( commandModule, commandName ) )  

  ## Private methods ###########################################################

  @staticmethod
  def __getVOBOXAvailabilityElems():
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
  
  @staticmethod
  def __getSpaceTokenOccupancyElems():
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

    for site, siteDict in spaceEndpoints.items():
      
      if not isinstance( siteDict, dict ):
        continue
      if not siteDict.has_key( 'Endpoint' ):
        continue
      
      for spaceToken in spaceTokens:

        elementsToCheck.append( { 'spaceTokenEndpoint' : siteDict[ 'Endpoint' ][0],
                                  'spaceToken'         : spaceToken
                                } )
    
    return S_OK( elementsToCheck )
  
  def __logVOBOXAvailabilityResults( self, results ):
    '''
      Save to database the results of the VOBOXAvailabilityCommand commands
    '''
    
    #FIXME: we need to delete entries in the database quite often, older than
    # ~30 min.
    
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

  def __logAccountingCacheResults( self, commandDict, results ):
    '''
      Save to database the results of the AccountingCacheCommand commands
    '''
           
    commandName = commandDict.keys()[ 0 ]
    
    plotType = commandDict[ commandName ][ 'plotType' ]  
    hours    = commandDict[ commandName ][ 'hours' ]

    plotName = '%s_%s' % ( commandName, hours )

    for name, value in results.items():

      resQuery = self.rmClient.addOrModifyAccountingCache( name, plotType, 
                                                           plotName, str( value ) )
      
      if not resQuery[ 'OK' ]:
        return resQuery
    
    return S_OK()  

  def __logDowntimeResults( self, commandDict, results ):
    '''
      Save to database the results of the DowntimeCommand commands
    '''
  
    commandName = commandDict.keys()[ 0 ]
  
    for downtime in results:
      
      # This returns either Site or Resource
      element = commandName.replace( 'Downtime', '' )[ :-1 ]
      
      try:
        
        iD          = downtime[ 'ID' ]
        name        = downtime[ 'Name' ]
        startDate   = downtime[ 'StartDate' ]
        endDate     = downtime[ 'EndDate' ]
        severity    = downtime[ 'Severity' ]
        description = downtime[ 'Description' ] 
        link        = downtime[ 'Link' ]
                
      except KeyError, e:
        return S_ERROR( e )
  
      resQuery = self.rmClient.addOrModifyDowntimeCache( iD, element, name, startDate, 
                                                         endDate, severity, description,
                                                         link )
  
      if not resQuery[ 'OK' ]:
        return resQuery    
  
    return S_OK()  

  def __logJobsResults( self, results ):
    '''
      Save to database the results of the JobsCommand commands
    '''

    for jobResult in results:
      
      try:
        
        site       = jobResult[ 'Site' ]
        maskStatus = jobResult[ 'MaskStatus' ]
        efficiency = jobResult[ 'Efficiency' ]
        status     = jobResult[ 'Status' ]
        
      except KeyError, e:
        return S_ERROR( e )  
      
      resQuery = self.rmClient.addOrModifyJobCache( site, maskStatus, efficiency, 
                                                    status )

      if not resQuery[ 'OK' ]:
        return resQuery    
  
    return S_OK()  
    
  def __logPilotsResults( self, results ):
    '''
      Save to database the results of the PilotsCommand commands
    '''

    for pilotResult in results:
      
      try:
        
        site         = pilotResult[ 'Site' ]
        cE           = pilotResult[ 'CE' ]
        pilotsPerJob = pilotResult[ 'PilotsPerJob' ]
        pilotJobEff  = pilotResult[ 'PilotJobEff' ]
        status       = pilotResult[ 'Status' ]
        
      except KeyError, e:
        return S_ERROR( e )  
      
      resQuery = self.rmClient.addOrModifyPilotCache( site, cE, pilotsPerJob, 
                                                      pilotJobEff, status )
      
      if not resQuery[ 'OK' ]:
        return resQuery    
  
    return S_OK()  

  def __logTransferResults( self, commandDict, results ):
    '''
      Save to database the results of the TransferCommand commands
    '''

    commandName = commandDict.keys()[ 0 ]

    direction = commandDict[ commandName ][ 'direction' ]
    #metric    = commandDict[ commandName ].keys()[0]
    metric    = commandName.replace( 'Transfer', '' )
    
    for elementName, transferResult in results.items():
      
      resQuery = self.rmClient.addOrModifyTransferCache( elementName, direction, 
                                                         metric, transferResult )
      
      if not resQuery[ 'OK' ]:
        return resQuery    
  
    return S_OK()  

  def __logSpaceTokenOccupancy( self, commandDict, commandObject, results ):
    
    spaceToken  = commandObject.args[ 'spaceToken' ]
    endpoint    = commandObject.args[ 'spaceTokenEndpoint' ]   

    total      = results[ 'total' ]
    guaranteed = results[ 'guaranteed' ]
    free       = results[ 'free' ]
    
    resQuery = self.rmClient.addOrModifySpaceTokenOccupancyCache( endpoint, spaceToken,
                                                                  total, guaranteed, 
                                                                  free )
    
    return resQuery  

  def __logGGUSTickets( self, results  ):
    
    failed, metrics = results
    
    self.log.info( 'Updated %s of %s' % ( metrics[ 'successful' ], metrics[ 'total' ] ) )
    if failed:
      self.log.warn( 'Failed %s' % len( failed ) )   
      self.log.warn( failed )
    
    return S_OK()  
     
      
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