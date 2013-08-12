# $HeadURL: $
''' JobCommand
  
  The JobCommand class is a command class to know about present jobs efficiency
  
'''

from datetime import datetime, timedelta

from DIRAC                                                      import S_OK, S_ERROR
from DIRAC.ResourceStatusSystem.Command.Command                 import Command
from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient
from DIRAC.ResourceStatusSystem.Utilities                       import CSHelpers
from DIRAC.WorkloadManagementSystem.DB.JobDB                    import JobDB

__RCSID__ = '$Id: $'


class JobCommand( Command ):
  '''
  Job "master" Command.
  '''


  def __init__( self, args = None, clients = None ):
    
    super( JobCommand, self ).__init__( args, clients )

    if 'JobDB' in self.apis:
      self.jobDB = self.apis[ 'JobDB' ]
    else:
      self.jobDB = JobDB()

    if 'ResourceManagementClient' in self.apis:
      self.rmClient = self.apis[ 'ResourceManagementClient' ]
    else:
      self.rmClient = ResourceManagementClient()


  def _storeCommand( self, result ):
    '''
    Stores the results of doNew method on the database.
    '''
    
    for jobDict in result:
      
      lowerCaseJobDict = {}
      for key, value in jobDict.iteritems():
        lowerCaseJobDict[ key[0].lower() + key[1:] ] = value
      
      resQuery = self.rmClient.addOrModifyJobCache( **lowerCaseJobDict )
      
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
     
    if not 'timespan' in self.args:
      return S_ERROR( '"timespan" not found in self.args' )
    timespan = self.args[ 'timespan' ]
  
    return S_OK( ( name, timespan ) )
  
  
  def doNew( self, masterParams = None ):
    '''
    Gets the parameters to run, either from the master method or from its
    own arguments.
    It contacts the WMSAdministrator with a list of site names, or a single
    site.
    If there are jobs, are recorded and then returned.
    '''
    
    if masterParams is True:
      self.args[ 'name' ] = ''

    params = self._prepareCommand()
    if not params[ 'OK' ]:
      return params

    name, timespan = params[ 'Value' ]
    
    condDict = {}
    if name:
      condDict = { 'Site' : name }

    startTimeWindow = datetime.utcnow() - timedelta( seconds = timespan )
    
    results = self.jobDB.getCounters( 'Jobs', ['Site', 'Status'],
                                      condDict, newer = startTimeWindow,
                                      timeStamp = 'LastUpdateTime' )
    
    if not results[ 'OK' ]:
      return results
    # Results look like this
    # [ ({'Status': 'Checking', 'Site': 'ANY'}, 6L), ...
    
    uniformResult = {}
    
    jobStatuses = ( 'Checking', 'Completed', 'Done', 'Failed', 'Killed', 'Matched',
                    'Received', 'Rescheduled', 'Running', 'Staging', 'Stalled',
                    'Waiting' )
    
    for resultTuple in results[ 'Value' ]:
      
      selectionDict, numberOfJobs = resultTuple
    
      siteName = selectionDict[ 'Site' ]
      
      if siteName in ( 'ANY', 'Multiple' ):
        continue
    
      if not siteName in uniformResult:
        uniformResult[ siteName ] = dict.fromkeys( jobStatuses, 0 )
      
      uniformResult[ siteName ][ selectionDict[ 'Status' ] ] = numberOfJobs

    # Store results
    storeRes = self._storeCommand( uniformResult )
    if not storeRes[ 'OK' ]:
      return storeRes
    
    return S_OK( uniformResult )
  
  
  def doCache( self ):
    '''
    Method that reads the cache table and tries to read from it. It will
    return a list of dictionaries if there are results.
    '''
    
    params = self._prepareCommand()
    if not params[ 'OK' ]:
      return params
    name = params[ 'Value' ]
    
    result = self.rmClient.selectJobCache( name )
    if result[ 'OK' ]:
      result = S_OK( [ dict( zip( result[ 'Columns' ], res ) ) for res in result[ 'Value' ] ] )
      
    return result
         
             
  def doMaster( self ):
    '''
    Master method.
    Gets all sites and calls doNew method.
    '''
        
    jobsResults = self.doNew( True )
    if not jobsResults[ 'OK' ]:
      self.metrics[ 'failed' ].append( jobsResults[ 'Message' ] )
      
    return S_OK( self.metrics )       
                 
################################################################################
################################################################################
################################################################################
################################################################################
################################################################################
################################################################################
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

#class JobsWMSCommand( Command ):
#  
#  def __init__( self, args = None, clients = None ):
#    
#    super( JobsWMSCommand, self ).__init__( args, clients )
#
#    if 'WMSAdministrator' in self.apis:
#      self.wmsAdmin = self.apis[ 'WMSAdministrator' ]
#    else:  
#      self.wmsAdmin = RPCClient( 'WorkloadManagement/WMSAdministrator' )
#  
#  def doCommand( self ):
#    """ 
#    Returns simple jobs efficiency
#
#    :attr:`args`: 
#       - args[0]: string: should be a ValidElement
#  
#       - args[1]: string should be the name of the ValidElement
#
#    returns:
#      {
#        'Result': 'Good'|'Fair'|'Poor'|'Idle'|'Bad'
#      }
#    """
#   
#    if not 'siteName' in self.args:
#      return self.returnERROR( S_ERROR( 'siteName is missing' ) )
#    siteName = self.args[ 'siteName' ]
#    
#    # If siteName is None, we take all sites
#    if siteName is None:
#      siteName = CSHelpers.getSites()      
#      if not siteName[ 'OK' ]:
#        return self.returnERROR( siteName )
#      siteName = siteName[ 'Value' ]
#    
#    results = self.wmsAdmin.getSiteSummaryWeb( { 'Site' : siteName }, [], 0, 500 )
#
#    if not results[ 'OK' ]:
#      return self.returnERROR( results )
#    results = results[ 'Value' ]
#    
#    if not 'ParameterNames' in results:
#      return self.returnERROR( S_ERROR( 'Malformed result dictionary' ) )
#    params = results[ 'ParameterNames' ]
#    
#    if not 'Records' in results:
#      return self.returnERROR( S_ERROR( 'Malformed result dictionary' ) )
#    records = results[ 'Records' ]
#    
#    jobResults = [] 
#       
#    for record in records:
#      
#      jobDict = dict( zip( params , record ))
#      try:
#        jobDict[ 'Efficiency' ] = float( jobDict[ 'Efficiency' ] )
#      except KeyError, e:
#        return self.returnERROR( S_ERROR( e ) )
#      except ValueError, e:
#        return self.returnERROR( S_ERROR( e ) )  
#      
#      jobResults.append( jobDict )
#    
#    return S_OK( jobResults )  

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

#class JobsEffSimpleCachedCommand( Command ):
#  
#  def __init__( self, args = None, clients = None ):
#    
#    super( JobsEffSimpleCachedCommand, self ).__init__( args, clients )
#          
#    if 'ResourceStatusClient' in self.apis:
#      self.rsClient = self.apis[ 'ResourceStatusClient' ]
#    else:
#      self.rsClient = ResourceStatusClient()  
#  
#    if 'ResourceManagementClient' in self.apis:
#      self.rmClient = self.apis[ 'ResourceManagementClient' ]
#    else:
#      self.rmClient = ResourceManagementClient()   
#  
#  def doCommand( self ):
#    """ 
#    Returns simple jobs efficiency
#
#    :attr:`args`: 
#       - args[0]: string: should be a ValidElement
#  
#       - args[1]: string should be the name of the ValidElement
#
#    returns:
#      {
#        'Result': 'Good'|'Fair'|'Poor'|'Idle'|'Bad'
#      }
#    """
#         
#    if self.args[0] == 'Service':
#      name = self.rsClient.getGeneralName( self.args[0], self.args[1], 'Site' )
#      name        = name[ 'Value' ][ 0 ]
#      granularity = 'Site'
#    elif self.args[0] == 'Site':
#      name        = self.args[1]
#      granularity = self.args[0]
#    else:
#      return S_ERROR( '%s is not a valid granularity' % self.args[ 0 ] )
#     
#    clientDict = { 
#                  'name'        : name,
#                  'commandName' : 'JobsEffSimpleEveryOne',
#                  'value'       : 'JE_S',
#                  'opt_ID'      : 'NULL',
#                  'meta'        : { 'columns'     : 'Result' }
#                  }
#      
#    res = self.rmClient.getClientCache( **clientDict )
#      
#    if res[ 'OK' ]:
#      res = res[ 'Value' ]
#      if res == None or res == []:
#        res = S_OK( 'Idle' )
#      else:
#        res = S_OK( res[ 0 ] )
#        
#    return res

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF