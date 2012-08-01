# $HeadURL:  $
''' JobsCommand
 
  The Jobs_Command class is a command class to know about 
  present jobs efficiency
  
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

#class JobsStatsCommand( Command ):
#  
#  def __init__( self, args = None, clients = None ):
#    
#    super( JobsStatsCommand, self ).__init__( args, clients )
#    
#    if 'JobsClient' in self.apis:
#      self.jClient = self.apis[ 'JobsClient' ]
#    else:
#      self.jClient = JobsClient()  
#  
#  def doCommand( self ):
#    """ 
#    Return getJobStats from Jobs Client  
#    
#   :attr:`args`: 
#     - args[0]: string: should be a ValidElement
#
#     - args[1]: string: should be the name of the ValidElement
#
#  returns:
#    {
#      'MeanProcessedJobs': X
#    }
#    """
#
#    return self.jClient.getJobsStats( self.args[0], self.args[1], self.args[2] )
    
################################################################################
################################################################################

#class JobsEffCommand( Command ):
#
#  def __init__( self, args = None, clients = None ):
#    
#    super( JobsEffCommand, self ).__init__( args, clients )
#    
#    if 'JobsClient' in self.apis:
#      self.jClient = self.apis[ 'JobsClient' ]
#    else:
#      self.jClient = JobsClient()  
#  
#  def doCommand( self ):
#    """ 
#    Return getJobsEff from Jobs Client  
#    
#   :attr:`args`: 
#       - args[0]: string: should be a ValidElement
#  
#       - args[1]: string: should be the name of the ValidElement
#
#    returns:
#      {
#        'JobsEff': X
#      }
#    """
#         
#    res = self.jClient.getJobsEff( self.args[0], self.args[1], self.args[2] )
#       
#    return S_OK( res )   

################################################################################
################################################################################

#class SystemChargeCommand( Command ):
#  
#  def __init__( self, args = None, clients = None ):
#    
#    super( SystemChargeCommand, self ).__init__( args, clients )
#    
#    if 'JobsClient' in self.apis:
#      self.jClient = self.apis[ 'JobsClient' ]
#    else:
#      self.jClient = JobsClient()  
#  
#  def doCommand(self):
#    """ Returns last hour system charge, and the system charge of an hour before
#
#        returns:
#          {
#            'LastHour': n_lastHour
#            'anHourBefore': n_anHourBefore
#          }
#    """
#    
#      
#    res = self.jClient.getSystemCharge()
#
#    return S_OK( res )   
    
################################################################################
################################################################################

class JobsWMSCommand( Command ):
  
  def __init__( self, args = None, clients = None ):
    
    super( JobsWMSCommand, self ).__init__( args, clients )

    if 'WMSAdministrator' in self.apis:
      self.wmsAdmin = self.apis[ 'WMSAdministrator' ]
    else:  
      self.wmsAdmin = RPCClient( 'WorkloadManagement/WMSAdministrator' )
  
  def doCommand( self ):
    """ 
    Returns simple jobs efficiency

    :attr:`args`: 
       - args[0]: string: should be a ValidElement
  
       - args[1]: string should be the name of the ValidElement

    returns:
      {
        'Result': 'Good'|'Fair'|'Poor'|'Idle'|'Bad'
      }
    """
   
    if not 'siteName' in self.args:
      return S_ERROR( 'siteName is missing' )
    siteName = self.args[ 'siteName' ]
    
    # If siteName is None, we take all sites
    if siteName is None:
      siteName = CSHelpers.getSites()      
      if not siteName[ 'OK' ]:
        return siteName
      siteName = siteName[ 'Value' ]
    
    results = self.wmsAdmin.getSiteSummaryWeb( { 'Site' : siteName }, [], 0, 500 )

    if not results[ 'OK' ]:
      return results
    results = results[ 'Value' ]
    
    if not 'ParameterNames' in results:
      return S_ERROR( 'Malformed result dictionary' )
    params = results[ 'ParameterNames' ]
    
    if not 'Records' in results:
      return S_ERROR( 'Malformed result dictionary' )
    records = results[ 'Records' ]
    
    jobResults = [] 
       
    for record in records:
      
      jobDict = dict( zip( params , record ))
      try:
        jobDict[ 'Efficiency' ] = float( jobDict[ 'Efficiency' ] )
      except KeyError, e:
        return S_ERROR( e )
      except ValueError, e:
        return S_ERROR( e )  
      
      jobResults.append( jobDict )
    
    return S_OK( jobResults )  

################################################################################
################################################################################

#class JobsEffSimpleEveryOneCommand( Command ):
#
#  #FIXME: write propper docstrings
#
#  def __init__( self, args = None, clients = None ):
#    
#    super( JobsEffSimpleEveryOneCommand, self ).__init__( args, clients )
#
#    if 'JobsClient' in self.apis:
#      self.jClient = self.apis[ 'JobsClient' ]
#    else:
#      self.jClient = JobsClient() 
#    
#  def doCommand( self ):
#    """ 
#    Returns simple jobs efficiency for all the sites in input.
#        
#    :params:
#      :attr:`sites`: list of site names (when not given, take every site)
#    
#    :returns:
#      {'SiteName': {'JE_S': 'Good'|'Fair'|'Poor'|'Idle'|'Bad'}, ...}
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
#        
#      if not sites['OK']:
#        return sites
#      sites = sites[ 'Value' ]   
#      #sites = [ site[ 0 ] for site in sites[ 'Value' ] ]
#
#    results = self.jClient.getJobsSimpleEff( sites )
#    
#    return results
#    
##    if not results[ 'OK' ]:
##      return results
##    results = results[ 'Value' ]
#        
##    if results is None:
##      results = {}
#
##    resToReturn = {}
#
#    #for site in results:
#    #  resToReturn[ site ] = results[ site ]
#
##    return S_OK( resToReturn )   

################################################################################
################################################################################ 

class JobsEffSimpleCachedCommand( Command ):
  
  def __init__( self, args = None, clients = None ):
    
    super( JobsEffSimpleCachedCommand, self ).__init__( args, clients )
          
    if 'ResourceStatusClient' in self.apis:
      self.rsClient = self.apis[ 'ResourceStatusClient' ]
    else:
      self.rsClient = ResourceStatusClient()  
  
    if 'ResourceManagementClient' in self.apis:
      self.rmClient = self.apis[ 'ResourceManagementClient' ]
    else:
      self.rmClient = ResourceManagementClient()   
  
  def doCommand( self ):
    """ 
    Returns simple jobs efficiency

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
      return S_ERROR( '%s is not a valid granularity' % self.args[ 0 ] )
     
    clientDict = { 
                  'name'        : name,
                  'commandName' : 'JobsEffSimpleEveryOne',
                  'value'       : 'JE_S',
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
        
    return res

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF