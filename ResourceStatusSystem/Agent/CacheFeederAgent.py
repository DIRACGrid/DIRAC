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

  def __init__( self, *args, **kwargs ):
    
    AgentModule.__init__( self, *args, **kwargs )
    
    self.commands = {}
    self.clients  = {} 
    
    self.rmClient = None   

  def initialize( self ):

    self.rmClient = ResourceManagementClient()

    self.commands[ 'Downtime' ]            = [ { 'Downtime'            : {} } ]
 
    
    #PilotsCommand
#    self.commands[ 'Pilots' ] = [ 
#                                 { 'PilotsWMS' : { 'element' : 'Site', 'siteName' : None } },
#                                 { 'PilotsWMS' : { 'element' : 'Resource', 'siteName' : None } } 
#                                 ]
        

    #FIXME: do not forget about hourly vs Always ...etc                                                                       
    #AccountingCacheCommand
#    self.commands[ 'AccountingCache' ] = [
#                                          {'SuccessfullJobsBySiteSplitted'    :{'hours' :24, 'plotType' :'Job' }},
#                                          {'FailedJobsBySiteSplitted'         :{'hours' :24, 'plotType' :'Job' }},
#                                          {'SuccessfullPilotsBySiteSplitted'  :{'hours' :24, 'plotType' :'Pilot' }},
#                                          {'FailedPilotsBySiteSplitted'       :{'hours' :24, 'plotType' :'Pilot' }},
#                                          {'SuccessfullPilotsByCESplitted'    :{'hours' :24, 'plotType' :'Pilot' }},
#                                          {'FailedPilotsByCESplitted'         :{'hours' :24, 'plotType' :'Pilot' }},
#                                          {'RunningJobsBySiteSplitted'        :{'hours' :24, 'plotType' :'Job' }},
##                                          {'RunningJobsBySiteSplitted'        :{'hours' :168, 'plotType' :'Job' }},
##                                          {'RunningJobsBySiteSplitted'        :{'hours' :720, 'plotType' :'Job' }},
##                                          {'RunningJobsBySiteSplitted'        :{'hours' :8760, 'plotType' :'Job' }},    
#                                          ]                                  
    
    #VOBOXAvailability
#    self.commands[ 'VOBOXAvailability' ] = [
#                                            { 'VOBOXAvailability' : {} }
#   
    
    #Reuse clients for the commands
    self.clients[ 'GOCDBClient' ]              = GOCDBClient()
    self.clients[ 'ReportGenerator' ]          = RPCClient( 'Accounting/ReportGenerator' )
    self.clients[ 'ReportsClient' ]            = ReportsClient()
    self.clients[ 'ResourceStatusClient' ]     = ResourceStatusClient()
    self.clients[ 'ResourceManagementClient' ] = ResourceManagementClient()
    self.clients[ 'WMSAdministrator' ]         = RPCClient( 'WorkloadManagement/WMSAdministrator' )

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
        commandObject.masterMode = True
        
        commandArgs[ 'command' ] = commandObject
        
        self.log.info( '%s loaded' % commandName )

    return S_OK()

  def execute( self ):        
      
    for commandModule, commandList in self.commands.items():
      
      for commandDict in commandList:
      
        commandName  = commandDict.keys()[0]
        commandArgs  = commandDict[ commandName ]
        commandObject = commandArgs[ 'command' ]

        self.log.info( '%s/%s' % ( commandModule, commandName ) )
          
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
    
    return extraArgs

  def logResults( self, commandModule, commandDict, commandObject, results ):
    '''
      Lazy method to run the appropiated method to log the results in the DB.
    '''

    return self.__logAccountingCacheResults( commandDict, results )

#    if commandModule == 'AccountingCache':
#      return self.__logAccountingCacheResults( commandDict, results ) 
#       
#    if commandModule == 'VOBOXAvailability':
#      return self.__logVOBOXAvailabilityResults( results )  
#
#    if commandModule == 'Downtime':
#      return self.__logResults( results )  
#    
#    if commandModule == 'Job':
#      return self.__logResults( results )
#      #return self.__logJobsResults( results )
#
#    if commandModule == 'Pilots':
#      return self.__logResults( results )
#      #return self.__logPilotsResults( results )
#
#    if commandModule == 'Transfer':
#      return self.__logResults( results )
#    
#    if commandModule == 'SpaceTokenOccupancy':
#      return self.__logResults( results )
#    
#    if commandModule == 'GGUSTickets':
#      return self.__logResults( results )
#
#    commandName = commandDict.keys()[ 0 ]
#    return S_ERROR( 'No log method for %s/%s' % ( commandModule, commandName ) )  

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

  def __logJobResults( self, results ):
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

  def __logResults( self, results ):
    '''
      Save to database the results of the TransferCommand commands
    '''

    if results[ 'failed' ]:   
      self.log.warn( results[ 'failed' ] )
    
    return S_OK()  
     
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
