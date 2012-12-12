# $HeadURL:  $
''' PilotCommand
 
  The PilotCommand class is a command class to know about present pilots 
  efficiency.
  
'''

from DIRAC                                                      import S_OK, S_ERROR
from DIRAC.Core.DISET.RPCClient                                 import RPCClient
from DIRAC.ResourceStatusSystem.Command.Command                 import Command
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient     import ResourceStatusClient 
from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient 
from DIRAC.ResourceStatusSystem.Utilities                       import CSHelpers

__RCSID__ = '$Id:  $'

class PilotCommand( Command ):
  '''
    Pilot "master" Command.    
  '''

  def __init__( self, args = None, clients = None ):
    
    super( PilotCommand, self ).__init__( args, clients )

    if 'WMSAdministrator' in self.apis:
      self.wmsAdmin = self.apis[ 'WMSAdministrator' ]
    else:  
      self.wmsAdmin = RPCClient( 'WorkloadManagement/WMSAdministrator' )

    if 'ResourceManagementClient' in self.apis:
      self.rmClient = self.apis[ 'ResourceManagementClient' ]
    else:
      self.rmClient = ResourceManagementClient()

  def _storeCommand( self, result ):
    '''
      Stores the results of doNew method on the database.
    '''
    
    for pilotDict in result:
      
      resQuery = self.rmClient.addOrModifyPilotCache( pilotDict[ 'Site' ], 
                                                      pilotDict[ 'CE' ], 
                                                      pilotDict[ 'PilotsPerJob' ], 
                                                      pilotDict[ 'PilotJobEff' ], 
                                                      pilotDict[ 'Status' ] )
      if not resQuery[ 'OK' ]:
        return resQuery

    return S_OK()
  
  def _prepareCommand( self ):
    '''
      JobCommand requires one arguments:
      - name : <str>      
    '''

    if not 'name' in self.args:
      return S_ERROR( '"name" not found in self.args' )
    name = self.args[ 'name' ]
  
    if not 'element' in self.args:
      return S_ERROR( 'element is missing' )
    element = self.args[ 'element' ]     
    
    if element not in [ 'Site', 'Resource' ]:
      return S_ERROR( '"%s" is not Site nor Resource' % element )
     
    return S_OK( ( element, name ) ) 
  
  def doNew( self, masterParams = None ):
  
    if masterParams is not None:
      element, name = masterParams
    else:
      params = self._prepareCommand()
      if not params[ 'OK' ]:
        return params
      element, name = params[ 'Value' ]
    
    wmsDict = {}
      
    if element == 'Site':
      wmsDict = { 'GridSite' : name }
    elif element == 'Resource':
      wmsDict = { 'ExpandSite' : name }
    else:
      # You should never see this error
      return S_ERROR( '"%s" is not  Site nor Resource' % element  )
      
    wmsResults = self.wmsAdmin.getPilotSummaryWeb( wmsDict, [], 0, 0 )

    if not wmsResults[ 'OK' ]:
      return wmsResults
    wmsResults = wmsResults[ 'Value' ]
    
    if not 'ParameterNames' in wmsResults:
      return S_ERROR( 'Wrong result dictionary, missing "ParameterNames"' )
    params = wmsResults[ 'ParameterNames' ]
    
    if not 'Records' in wmsResults:
      return S_ERROR( 'Wrong formed result dictionary, missing "Records"' )
    records = wmsResults[ 'Records' ]
    
    uniformResult = [] 
       
    for record in records:
      
      # This returns a dictionary with the following keys:
      # 'Site', 'CE', 'Submitted', 'Ready', 'Scheduled', 'Waiting', 'Running', 
      # 'Done', 'Aborted', 'Done_Empty', 'Aborted_Hour', 'Total', 'PilotsPerJob', 
      # 'PilotJobEff', 'Status', 'InMask'
      pilotDict = dict( zip( params, record ) )
      
      pilotDict[ 'PilotsPerJob' ] = float( pilotDict[ 'PilotsPerJob' ] )
      pilotDict[ 'PilotJobEff' ]  = float( pilotDict[ 'PilotJobEff' ] )
      
      uniformResult.append( pilotDict )
    
    storeRes = self._storeCommand( uniformResult )
    if not storeRes[ 'OK' ]:
      return storeRes
    
    return S_OK( uniformResult )   

  def doCache( self ):
 
    params = self._prepareCommand()
    if not params[ 'OK' ]:
      return params
    element, name = params[ 'Value' ]   
    
    if element == 'Site':
      # WMS returns Site entries with CE = 'Multiple'
      site, ce = name, 'Multiple'
    elif element == 'Resource':
      site, ce = None, name
    else:  
      # You should never see this error
      return S_ERROR( '"%s" is not  Site nor Resource' % element  )      

    result = self.rmClient.selectPilotCache( site, ce )  
    if result[ 'OK' ]:
      result = S_OK( [ dict( zip( result[ 'Columns' ], res ) ) for res in result[ 'Value' ] ] )
      
    return result    

  def doMaster( self ):
    
    siteNames = CSHelpers.getSites()
    if not siteNames[ 'OK' ]:
      return siteNames
    siteNames = siteNames[ 'Value' ]
    
    ces = CSHelpers.getComputingElements()
    if not ces[ 'OK' ]:
      return ces
    ces = ces[ 'Value' ]
    
    pilotResults = self.doNew( ( 'Site', siteNames ) )
    if not pilotResults[ 'OK' ]:
      self.metrics[ 'failed' ].append( pilotResults[ 'Message' ] )
    
    pilotResults = self.doNew( ( 'Resource', ces ) )
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
      siteName = CSHelpers.getSites()      
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

class PilotsEffSimpleCachedCommand( Command ):

  def __init__( self, args = None, clients = None ):
    
    super( PilotsEffSimpleCachedCommand, self ).__init__( args, clients )
    
    if 'ResourceStatusClient' in self.apis:
      self.rsClient = self.apis[ 'ResourceStatusClient' ]
    else:
      self.rsClient = ResourceStatusClient()      
    
    if 'ResourceManagementClient' in self.apis:
      self.rmClient = self.apis[ 'ResourceManagementClient' ]
    else:
      self.emClient = ResourceManagementClient()  

  def doCommand( self ):
    """
    Returns simple pilots efficiency

    :attr:`args`:
       - args[0]: string: should be a ValidElement

       - args[1]: string should be the name of the ValidElement

    returns:
      {
        'Result': 'Good'|'Fair'|'Poor'|'Idle'|'Bad'
      }
    """
    
    if self.args[0] == 'Service':
      name = self.rsClient.getGeneralName( self.args[0], self.args[1], 'Site' )
      name        = name[ 'Value' ][ 0 ]
      granularity = 'Site'
    elif self.args[0] == 'Site':
      name        = self.args[1]
      granularity = self.args[0]
    else:
      return self.returnERROR( S_ERROR( '%s is not a valid granularity' % self.args[ 0 ] ) )

    clientDict = { 
                  'name'        : name,
                  'commandName' : 'PilotsEffSimpleEverySites',
                  'value'       : 'PE_S',
                  'opt_ID'      : 'NULL',
                  'meta'        : { 'columns'     : 'Result' }
                  }
      
    res = self.rmClient.getClientCache( **clientDict )
      
    if res[ 'OK' ]:               
      res = res[ 'Value' ] 
      if res == None or res == []:
        res = S_OK( 'Idle' )
      else:
        res = S_OK( res[ 0 ] )

    else:
      res = self.returnERROR( res )

    return res

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF  