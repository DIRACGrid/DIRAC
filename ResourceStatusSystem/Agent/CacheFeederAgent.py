# $HeadURL:  $
''' CacheFeederAgent

  This agent feeds the Cache tables with the outputs of the cache commands.

'''

from DIRAC                                                      import S_OK
from DIRAC.AccountingSystem.Client.ReportsClient                import ReportsClient
from DIRAC.Core.Base.AgentModule                                import AgentModule
from DIRAC.Core.DISET.RPCClient                                 import RPCClient
from DIRAC.Core.LCG.GOCDBClient                                 import GOCDBClient
from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient     import ResourceStatusClient
from DIRAC.ResourceStatusSystem.Command                         import CommandCaller
from DIRAC.WorkloadManagementSystem.DB.PilotAgentsDB            import PilotAgentsDB

__RCSID__  = '$Id:  $'
AGENT_NAME = 'ResourceStatus/CacheFeederAgent'

class CacheFeederAgent( AgentModule ):
  '''
  The CacheFeederAgent feeds the cache tables for the client and the accounting.
  It runs periodically a set of commands, and stores it's results on the
  tables.
  '''

  # Too many public methods
  # pylint: disable=R0904  

  def __init__( self, *args, **kwargs ):
    
    AgentModule.__init__( self, *args, **kwargs )
    
    self.commands = {}
    self.clients  = {} 
    
    self.cCaller  = None
    self.rmClient = None

  def initialize( self ):

    self.am_setOption( 'shifterProxy', 'DataManager' )

    self.rmClient = ResourceManagementClient()

    self.commands[ 'Downtime' ]            = [ { 'Downtime'            : {} } ]
    self.commands[ 'SpaceTokenOccupancy' ] = [ { 'SpaceTokenOccupancy' : {} } ]
    self.commands[ 'Pilot' ]               = [ { 'Pilot' : { 'timespan' : 1800 } },]
#                                               { 'Pilot' : { 'timespan' : 86400 } },
#                                               { 'Pilot' : { 'timespan' : 604800 } }]
 
    
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
    self.clients[ 'PilotsDB' ]                 = PilotAgentsDB()
    self.clients[ 'WMSAdministrator' ]         = RPCClient( 'WorkloadManagement/WMSAdministrator' )

    self.cCaller = CommandCaller
    
    return S_OK()

  def loadCommand( self, commandModule, commandDict ):

    commandName = commandDict.keys()[ 0 ]
    commandArgs = commandDict[ commandName ]

    commandTuple  = ( '%sCommand' % commandModule, '%sCommand' % commandName )
    commandObject = self.cCaller.commandInvocation( commandTuple, pArgs = commandArgs,
                                                    clients = self.clients )
        
    if not commandObject[ 'OK' ]:
      self.log.error( 'Error initializing %s' % commandName )
      return commandObject
    commandObject = commandObject[ 'Value' ]
    
    # Set master mode
    commandObject.masterMode = True
        
    self.log.info( '%s/%s' % ( commandModule, commandName ) )

    return S_OK( commandObject )
        

  def execute( self ):
      
    for commandModule, commandList in self.commands.items():
      
      self.log.info( '%s module initialization' % commandModule )
      
      for commandDict in commandList:
      
        commandObject = self.loadCommand( commandModule, commandDict )
        if not commandObject[ 'OK' ]:
          self.log.error( commandObject[ 'Message' ] )
          continue
        commandObject = commandObject[ 'Value' ]
                  
        results = commandObject.doCommand()
                    
        if not results[ 'OK' ]:
          self.log.error( results[ 'Message' ] )
          continue
        results = results[ 'Value' ]

        if not results:
          self.log.info( 'Empty results' )
          continue
          
        self.log.verbose( 'Command OK Results' )  
        self.log.verbose( results )
          
    return S_OK()  
     
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF