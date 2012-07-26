# $HeadURL:  $
''' JobsCommand
 
  The Jobs_Command class is a command class to know about 
  present jobs efficiency
  
'''

from DIRAC                                                      import S_OK, S_ERROR
from DIRAC.ResourceStatusSystem.Command.Command                 import Command
from DIRAC.ResourceStatusSystem.Client.JobsClient               import JobsClient
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient     import ResourceStatusClient
from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient

__RCSID__ = '$Id:  $'

################################################################################
################################################################################

class JobsStatsCommand( Command ):
  
  def __init__( self, args = None, clients = None ):
    
    super( JobsStatsCommand, self ).__init__( args, clients )
    
    if 'JobsClient' in self.apis:
      self.jClient = self.apis[ 'JobsClient' ]
    else:
      self.jClient = JobsClient()  
  
  def doCommand( self ):
    """ 
    Return getJobStats from Jobs Client  
    
   :attr:`args`: 
     - args[0]: string: should be a ValidElement

     - args[1]: string: should be the name of the ValidElement

  returns:
    {
      'MeanProcessedJobs': X
    }
    """

    return self.jClient.getJobsStats( self.args[0], self.args[1], self.args[2] )
    
################################################################################
################################################################################

class JobsEffCommand( Command ):

  def __init__( self, args = None, clients = None ):
    
    super( JobsEffCommand, self ).__init__( args, clients )
    
    if 'JobsClient' in self.apis:
      self.jClient = self.apis[ 'JobsClient' ]
    else:
      self.jClient = JobsClient()  
  
  def doCommand( self ):
    """ 
    Return getJobsEff from Jobs Client  
    
   :attr:`args`: 
       - args[0]: string: should be a ValidElement
  
       - args[1]: string: should be the name of the ValidElement

    returns:
      {
        'JobsEff': X
      }
    """
         
    res = self.jClient.getJobsEff( self.args[0], self.args[1], self.args[2] )
       
    return S_OK( res )   

################################################################################
################################################################################

class SystemChargeCommand( Command ):
  
  def __init__( self, args = None, clients = None ):
    
    super( SystemChargeCommand, self ).__init__( args, clients )
    
    if 'JobsClient' in self.apis:
      self.jClient = self.apis[ 'JobsClient' ]
    else:
      self.jClient = JobsClient()  
  
  def doCommand(self):
    """ Returns last hour system charge, and the system charge of an hour before

        returns:
          {
            'LastHour': n_lastHour
            'anHourBefore': n_anHourBefore
          }
    """
    
      
    res = self.jClient.getSystemCharge()

    return S_OK( res )   
    
################################################################################
################################################################################

class JobsEffSimpleCommand( Command ):
  
  def __init__( self, args = None, clients = None ):
    
    super( JobsEffSimpleCommand, self ).__init__( args, clients )
    
    if 'JobsClient' in self.apis:
      self.jClient = self.apis[ 'JobsClient' ]
    else:
      self.jClient = JobsClient()  
      
    if 'ResourceStatusClient' in self.apis:
      self.rsClient = self.apis[ 'ResourceStatusClient' ]
    else:
      self.rsClient = ResourceStatusClient()  
  
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
    
    if not 'element' in self.args:
      return S_ERROR( 'element is missing' )
    element = self.args[ 'element' ]
    
    #FIXME: maybe a Service as well ??
    if element != 'Site':
      return S_ERROR( 'Expecting site' )
    
    if not 'name' in self.args:
      return S_ERROR( 'name is missing' )
    name = self.args[ 'name' ]   
       
#    if element == 'Service':
#      name    = self.rsClient.getGeneralName( element, name, 'Site' )    
#      name    = name[ 'Value' ][ 0 ]
#      element = 'Site'
#    elif element == 'Site':
#      pass
##      name        = self.args[1]
##      granularity = self.args[0]
#    else:
#      return S_ERROR( '%s is not a valid element' % element )
         
    results = self.jClient.getJobsSimpleEff( name )
    if not results[ 'OK' ]:
      return results
    results = results[ 'Value' ]
     
    #FIXME: can it actually return None ? 
    if results == None:
      results = 'Idle'
    else:
      results = results[ name ] 
    
    return S_OK( results )
   
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