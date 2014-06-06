""" PilotCommand
 
  The PilotCommand class is a command class to know about present pilots 
  efficiency. It reads from the PilotAgentsDB the values of the number of pilots
  for a given timespan.
  
"""

from DIRAC                                                      import S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Helpers                   import Resources
from DIRAC.Core.DISET.RPCClient                                 import RPCClient
from DIRAC.ResourceStatusSystem.Command.Command                 import Command
from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient 
from DIRAC.WorkloadManagementSystem.DB.PilotAgentsDB            import PilotAgentsDB

__RCSID__ = '$Id:  $'

class PilotCommand( Command ):
  """
    Pilot "master" Command.    
  """

  def __init__( self, args = None, clients = None ):
    """ Constructor.
    
    :Parameters:
      **args** - [, `dict` ]
        arguments to be passed to be used in the _prepareCommand method ( name and
        timespan are the expected ones )
      **clients - [, `dict` ]
        clients from where information is fetched. Mainly used to avoid creating
        new connections on agents looping over clients. ResourceManagementClient
        and PilotsDB are most welcome.  
    """
    
    super( PilotCommand, self ).__init__( args, clients )

    if 'PilotsDB' in self.apis:
      self.pilotsDB = self.apis[ 'PilotsDB' ]
    else:
      self.pilotsDB = PilotAgentsDB()

    if 'ResourceManagementClient' in self.apis:
      self.rmClient = self.apis[ 'ResourceManagementClient' ]
    else:
      self.rmClient = ResourceManagementClient()

  def _storeCommand( self, result ):
    """
      Stores the results of doNew method on the database.
    """
    
    for pilotDict in result:
      
      lowerCasePilotDict = {}
      for key, value in pilotDict.iteritems():
        lowerCasePilotDict[ key[0].lower() + key[1:] ] = value
      
      # I do not care about the **magic, it makes it cleaner
      resQuery = self.rmClient.addOrModifyPilotCache( **lowerCasePilotDict )
      if not resQuery[ 'OK' ]:
        return resQuery

    return S_OK()
  
  def _prepareCommand( self ):
    """
      JobCommand requires one arguments:
      - name : <str>      
    """

    if not 'name' in self.args:
      return S_ERROR( '"name" not found in self.args' )
    name = self.args[ 'name' ]
  
    if not 'timespan' in self.args:
      return S_ERROR( '"timespan" not found in self.args' )
    timespan = self.args[ 'timespan' ]
  
    return S_OK( ( name, timespan ) )
  
  def doNew( self, masterParams = None ):
    """ doNew method. If is master execution, name is declared as '' so that 
    all ce's are asked. Once values are obtained, they are stored on the Database.
    The entries with name Unknown, NotAssigned and Total are skipped.

    :Parameters:
      **masterParams** - [, bool ]
        if True, it queries for all elements in the database for the given timespan
    
    :return: S_OK( list ( dict ) ) / S_ERROR
    """
    
    # Ask for all CEs
    if masterParams is True:
      self.args[ 'name' ] = ''

    params = self._prepareCommand()
    if not params[ 'OK' ]:
      return params
    computingElement, timespan = params[ 'Value' ]
  
    # Calculate time window from timespan and utcnow  
    endTimeWindow   = datetime.utcnow()
    startTimeWindow = endTimeWindow - timedelta( seconds = timespan )
  
    # Get pilots information from DB 
    pilotsRes = self.pilotsDB.getPilotSummaryShort( startTimeWindow, 
                                                    endTimeWindow, 
                                                    computingElement )
    if not pilotsRes[ 'OK' ]:
      return pilotsRes
 
    # This list matches the database schema in ResourceManagemntDB. It is used
    # to have a perfect match even it there are no pilots on a particular state
    pilotStatuses = [ 'Scheduled', 'Waiting', 'Submitted', 'Running', 'Done', 'Aborted', 
                      'Cancelled', 'Deleted', 'Failed', 'Held', 'Killed', 'Stalled' ]
 
    uniformResult = [] 
       
    for ceName, pilotDict in pilotsRes[ 'Value' ].items():
      
      if ceName in [ 'Total', 'Unknown', 'NotAssigned' ]:
        continue
      
      uniformPilotDict = dict.fromkeys( pilotStatuses, 0 )
      uniformPilotDict.update( pilotDict )
      uniformPilotDict[ 'Timespan' ] = timespan
      uniformPilotDict[ 'CE' ]       = ceName
            
      uniformResult.append( uniformPilotDict )
    
    # Store results
    storeRes = self._storeCommand( uniformResult )
    if not storeRes[ 'OK' ]:
      return storeRes
    
    return S_OK( uniformResult )   

  def doCache( self ):
    """ doCache gets values from the database instead from the PilotsDB tables.
    If successful, returns a list of dictionaries with the database records. 
     
    :return: S_OK( list( dict ) ) / S_ERROR 
    """
 
    params = self._prepareCommand()
    if not params[ 'OK' ]:
      return params
    computingElement, timespan = params[ 'Value' ]    

    # Make sure the records we obtain are NOT out of date
    lastValidRecord = datetime.utcnow() - timedelta( seconds = timespan )

    result = self.rmClient.selectPilotCache( cE = computingElement, timespan = timespan,
                                             meta = { 'older' : ( 'LastCheckTime', lastValidRecord ) } )  
    if result[ 'OK' ]:
      result = S_OK( [ dict( zip( result[ 'Columns' ], res ) ) for res in result[ 'Value' ] ] )
      
    return result    

  def doMaster( self ):
    """ Master method, asks for all information in the database for the given 
    timespan ( see _prepareCommand ).

    :return: : S_OK( failedMessages )
    """
    
    pilotResults = self.doNew( masterParams = True )
    if not pilotResults[ 'OK' ]:
      self.metrics[ 'failed' ].append( pilotResults[ 'Message' ] )    
        
    return S_OK( self.metrics )    
        
################################################################################
################################################################################

#class PilotsStatsCommand( Command ):
#
#  def __init__( self, args = None, clients = None ):
#    
#    super( PilotsStatsCommand, self ).__init__( args, clients )
#    
#    if 'PilotsClient' in self.apis:
#      self.pClient = self.apis[ 'PilotsClient' ]
#    else:
#      self.pClient = PilotsClient()     
#
#  def doCommand( self ):
#    """
#    Return getPilotStats from Pilots Client
#    """
#    
#    return self.pClient.getPilotsStats( self.args[0], self.args[1], self.args[2] )

################################################################################
################################################################################

#class PilotsEffCommand( Command ):
#
#  def __init__( self, args = None, clients = None ):
#    
#    super( PilotsEffCommand, self ).__init__( args, clients )
#    
#    if 'PilotsClient' in self.apis:
#      self.pClient = self.apis[ 'PilotsClient' ]
#    else:
#      self.pClient = PilotsClient()  
#
#  def doCommand( self ):
#    """
#    Return getPilotsEff from Pilots Client
#    """
#          
#    return self.pClient.getPilotsEff( self.args[0], self.args[1], self.args[2] )

################################################################################
################################################################################

class PilotsWMSCommand( Command ):

  def __init__( self, args = None, clients = None ):
    
    super( PilotsWMSCommand, self ).__init__( args, clients )
    
    if 'WMSAdministrator' in self.apis:
      self.wmsAdmin = self.apis[ 'WMSAdministrator' ]
    else:  
      self.wmsAdmin = RPCClient( 'WorkloadManagement/WMSAdministrator' )

  def doCommand( self ):
    """
#    Returns simple pilots efficiency
#
#    :attr:`args`:
#        - args[0]: string - should be a ValidElement
#
#        - args[1]: string - should be the name of the ValidElement
#
#    returns:
#      {
#        'Result': 'Good'|'Fair'|'Poor'|'Idle'|'Bad'
#      }
    """

    if not 'element' in self.args:
      return self.returnERROR( S_ERROR( 'element is missing' ) )
    element = self.args[ 'element' ]    
   
    if not 'siteName' in self.args:
      return self.returnERROR( S_ERROR( 'siteName is missing' ) )
    siteName = self.args[ 'siteName' ]  
    
    # If siteName is None, we take all sites
    if siteName is None:
      siteName = Resources.getSites()      
      if not siteName[ 'OK' ]:
        return self.returnERROR( siteName )
      siteName = siteName[ 'Value' ]

    if element == 'Site':
      results = self.wmsAdmin.getPilotSummaryWeb( { 'GridSite' : siteName }, [], 0, 300 )
    elif element == 'Resource':
      results = self.wmsAdmin.getPilotSummaryWeb( { 'ExpandSite' : siteName }, [], 0, 300 )      
    else:
      return self.returnERROR( S_ERROR( '%s is a wrong element' % element ) )  
       
    if not results[ 'OK' ]:
      return self.returnERROR( results )
    results = results[ 'Value' ]
    
    if not 'ParameterNames' in results:
      return self.returnERROR( S_ERROR( 'Malformed result dictionary' ) )
    params = results[ 'ParameterNames' ]
    
    if not 'Records' in results:
      return self.returnERROR( S_ERROR( 'Malformed result dictionary' ) )
    records = results[ 'Records' ]
    
    pilotResults = [] 
       
    for record in records:
      
      pilotDict = dict( zip( params , record ))
      try:
        pilotDict[ 'PilotsPerJob' ] = float( pilotDict[ 'PilotsPerJob' ] )
        pilotDict[ 'PilotsJobEff' ] = float( pilotDict[ 'PilotsJobEff' ] )
      except KeyError, e:
        return self.returnERROR( S_ERROR( e ) ) 
      except ValueError, e:
        return self.returnERROR( S_ERROR( e ) )
      
      pilotResults.append( pilotDict )
    
    return S_OK( pilotResults )

################################################################################
################################################################################

#class PilotsEffSimpleEverySitesCommand( Command ):
#
#  #FIXME: write propper docstrings
#
#  def __init__( self, args = None, clients = None ):
#    
#    super( PilotsEffSimpleEverySitesCommand, self ).__init__( args, clients )
#
#    if 'PilotsClient' in self.apis:
#      self.pClient = self.apis[ 'PilotsClient' ]
#    else:
#      self.pClient = PilotsClient() 
#
#  def doCommand( self ):
#    """ 
#    Returns simple pilots efficiency for all the sites and resources in input.
#        
#    :params:
#      :attr:`sites`: list of site names (when not given, take every site)
#    
#    :returns:
#      {'SiteName':  {'PE_S': 'Good'|'Fair'|'Poor'|'Idle'|'Bad'} ...}
#    """
#
#    sites = None
#
#    if 'sites' in self.args:
#      sites = self.args[ 'sites' ] 
#
#    if sites is None:
#      #FIXME: we do not get them from RSS DB anymore, from CS now.
#      #sites = self.rsClient.selectSite( meta = { 'columns' : 'SiteName' } )
#      sites = CSHelpers.getSites()      
#      if not sites[ 'OK' ]:
#        return sites
#      sites = sites[ 'Value' ]
#
#    results = self.pClient.getPilotsSimpleEff( 'Site', sites, None )
#    
#    return results

################################################################################
################################################################################


# class PilotsEffSimpleCachedCommand( Command ):
#
#   def __init__( self, args = None, clients = None ):
#
#     super( PilotsEffSimpleCachedCommand, self ).__init__( args, clients )
#
#     if 'ResourceStatusClient' in self.apis:
#       self.rsClient = self.apis[ 'ResourceStatusClient' ]
#     else:
#       self.rsClient = ResourceStatusClient()
#
#     if 'ResourceManagementClient' in self.apis:
#       self.rmClient = self.apis[ 'ResourceManagementClient' ]
#     else:
#       self.emClient = ResourceManagementClient()
#
#   def doCommand( self ):
#     """
#     Returns simple pilots efficiency
#
#     :attr:`args`:
#        - args[0]: string: should be a ValidElement
#
#        - args[1]: string should be the name of the ValidElement
#
#     returns:
#       {
#         'Result': 'Good'|'Fair'|'Poor'|'Idle'|'Bad'
#       }
#     """
#
#     if self.args[0] == 'Service':
#       name = self.rsClient.getGeneralName( self.args[0], self.args[1], 'Site' )
#       name        = name[ 'Value' ][ 0 ]
#       granularity = 'Site'
#     elif self.args[0] == 'Site':
#       name        = self.args[1]
#       granularity = self.args[0]
#     else:
#       return self.returnERROR( S_ERROR( '%s is not a valid granularity' % self.args[ 0 ] ) )
#
#     clientDict = {
#                   'name'        : name,
#                   'commandName' : 'PilotsEffSimpleEverySites',
#                   'value'       : 'PE_S',
#                   'opt_ID'      : 'NULL',
#                   'meta'        : { 'columns'     : 'Result' }
#                   }
#
#     res = self.rmClient.getClientCache( **clientDict )
#
#     if res[ 'OK' ]:
#       res = res[ 'Value' ]
#       if res == None or res == []:
#         res = S_OK( 'Idle' )
#       else:
#         res = S_OK( res[ 0 ] )
#
#     else:
#       res = self.returnERROR( res )
#
#     return res

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF  
