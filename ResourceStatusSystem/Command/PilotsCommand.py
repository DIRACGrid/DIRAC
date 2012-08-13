# $HeadURL:  $
''' PilotsCommand
 
  The PilotsCommand class is a command class to know about
  present pilots efficiency.
  
'''

from DIRAC                                                      import S_OK, S_ERROR
from DIRAC.Core.DISET.RPCClient                                 import RPCClient
from DIRAC.ResourceStatusSystem.Command.Command                 import Command
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient     import ResourceStatusClient 
from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient 
from DIRAC.ResourceStatusSystem.Utilities                       import CSHelpers

__RCSID__ = '$Id:  $'

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