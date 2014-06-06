# $HeadURL:  $
''' RSCommand

'''

from DIRAC                                                  import S_OK, S_ERROR
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
from DIRAC.ResourceStatusSystem.Command.Command             import Command
from DIRAC.ResourceStatusSystem.Utilities                   import RssConfiguration

__RCSID__ = '$Id:  $'

################################################################################
################################################################################

class RSPeriodsCommand( Command ):
  pass
#  def __init__( self, args = None, clients = None ):
#    
#    super( RSPeriodsCommand, self ).__init__( args, clients )
#    
#    if 'ResourceStatusClient' in self.apis:
#      self.rsClient = self.apis[ 'ResourceStatusClient' ]
#    else:
#      self.rsClient = ResourceStatusClient()      
#
#  def doCommand( self ):
#    """
#    Return getPeriods from ResourceStatus Client
#
#    - args[0] should be a ValidElement
#
#    - args[1] should be the name of the ValidElement
#
#    - args[2] should be the present status
#
#    - args[3] are the number of hours requested
#    """
#
##    try:
#      
#    res = self.rsClient.getPeriods( self.args[0], self.args[1], self.args[2], self.args[3] )
#    
##    except Exception, e:
##      _msg = '%s (%s): %s' % ( self.__class__.__name__, self.args, e )
##      gLogger.exception( _msg )
##      return S_ERROR( _msg )
#
#    return res

################################################################################
################################################################################

class ServiceStatsCommand( Command ):
  """
  The ServiceStats_Command class is a command class to know about
  present services stats
  """
  pass
#  def __init__( self, args = None, clients = None ):
#    
#    super( ServiceStatsCommand, self ).__init__( args, clients )
#    
#    if 'ResourceStatusClient' in self.apis:
#      self.rsClient = self.apis[ 'ResourceStatusClient' ]
#    else:
#      self.rsClient = ResourceStatusClient()   
#
#  def doCommand( self ):
#    """
#    Uses :meth:`DIRAC.ResourceStatusSystem.Client.ResourceStatusClient.getServiceStats`
#
#    :params:
#      :attr:`args`: a tuple
#        - args[1]: a ValidElement
#
#        - args[0]: should be the name of the Site
#
#    :returns:
#      {'Active':xx, 'Probing':yy, 'Banned':zz, 'Total':xyz}
#    """
#
##    try:
#      
#    res = self.rsClient.getServiceStats( self.args[1] )#, statusType = None )# self.args[0], self.args[1] )['Value']
#    if not res[ 'OK' ]:
#      return self.returnERROR( res )
##    except Exception, e:
##      _msg = '%s (%s): %s' % ( self.__class__.__name__, self.args, e )
##      gLogger.exception( _msg )
##      return S_ERROR( _msg )
#
#    return res

################################################################################
################################################################################

class ResourceStatsCommand( Command ):
  """
  The ResourceStats_Command class is a command class to know about
  present resources stats
  """
  pass
#  def __init__( self, args = None, clients = None ):
#    
#    super( ResourceStatsCommand, self ).__init__( args, clients )
#    
#    if 'ResourceStatusClient' in self.apis:
#      self.rsClient = self.apis[ 'ResourceStatusClient' ]
#    else:
#      self.rsClient = ResourceStatusClient()  
#
#  def doCommand( self ):
#    """
#    Uses :meth:`DIRAC.ResourceStatusSystem.Client.ResourceStatusClient.getResourceStats`
#
#    :params:
#      :attr:`args`: a tuple
#        - `args[0]` string, a ValidElement. Should be in ('Site', 'Service')
#
#        - `args[1]` should be the name of the Site or Service
#
#    :returns:
#
#    """
#
##    try:
#      
#    res = self.rsClient.getResourceStats( self.args[0], self.args[1], statusType = None )
#    if not res[ 'OK' ]:
#      return self.returnERROR( res )
##    except Exception, e:
##      _msg = '%s (%s): %s' % ( self.__class__.__name__, self.args, e )
##      gLogger.exception( _msg )
##      return S_ERROR( _msg )
#
#    return res

################################################################################
################################################################################

class StorageElementsStatsCommand( Command ):
  """
  The StorageElementsStats_Command class is a command class to know about
  present storageElementss stats
  """
  pass
#  def __init__( self, args = None, clients = None ):
#    
#    super( StorageElementsStatsCommand, self ).__init__( args, clients )
#    
#    if 'ResourceStatusClient' in self.apis:
#      self.rsClient = self.apis[ 'ResourceStatusClient' ]
#    else:
#      self.rsClient = ResourceStatusClient()  
#
#  def doCommand( self ):
#    """
#    Uses :meth:`DIRAC.ResourceStatusSystem.Client.ResourceStatusClient.getStorageElementStats`
#
#    :params:
#      :attr:`args`: a tuple
#        - `args[0]` should be in ['Site', 'Resource']
#
#        - `args[1]` should be the name of the Site or Resource
#
#    :returns:
#
#    """
#
##    try:
#
#    if self.args[0] == 'Service':
#      granularity = 'Site'
#      name        = self.args[1].split( '@' )[1]
#    elif self.args[0] in [ 'Site', 'Resource' ]:
#      granularity = self.args[0]
#      name        = self.args[1]
#    else:
#      return self.returnERROR( S_ERROR( '%s is not a valid granularity' % self.args[ 0 ] ) )
#
#    res = self.rsClient.getStorageElementStats( granularity, name, statusType = None )
#    if not res[ 'OK' ]:
#      return self.returnERROR( res )  
##    except Exception, e:
##      _msg = '%s (%s): %s' % ( self.__class__.__name__, self.args, e )
##      gLogger.exception( _msg )
##      return S_ERROR( _msg )
#
#    return res

#  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

################################################################################
################################################################################

class MonitoredStatusCommand( Command ):
  """
  The MonitoredStatus_Command class is a command class to know about
  monitored status.
  """
  pass
#  def __init__( self, args = None, clients = None ):
#    
#    super( MonitoredStatusCommand, self ).__init__( args, clients )
#    
#    if 'ResourceStatusClient' in self.apis:
#      self.rsClient = self.apis[ 'ResourceStatusClient' ]
#    else:
#      self.rsClient = ResourceStatusClient()  
#
#  def doCommand( self ):
#    """
#    Uses :meth:`DIRAC.ResourceStatusSystem.Client.ResourceStatusClient.getMonitoredStatus`
#
#    :params:
#      :attr:`args`: a tuple
#        - `args[0]`: string          - should be a ValidElement
#        - `args[1]`: string          - should be the name of the ValidElement
#        - `args[2]`: optional string - a ValidElement (get status of THIS ValidElement
#          for name in args[1], will call getGeneralName)
#
#    :returns:
#      {'MonitoredStatus': 'Active'|'Probing'|'Banned'}
#    """
#
##    try:
#
#    validElements = RssConfiguration.getValidElements()
#
#    if len( self.args ) == 3:
#      if validElements.index( self.args[2] ) >= validElements.index( self.args[0] ):
#        return self.returnERROR( S_ERROR( 'Error in MonitoredStatus_Command' ) )
#      toBeFound = self.rsClient.getGeneralName( self.args[0], self.args[1], self.args[2] )[ 'Value' ]
#    else:
#      toBeFound = self.args[1]
#
#    res = self.rsClient.getMonitoredStatus( self.args[2], toBeFound )
#    if res[ 'OK' ]:
#      res = res[ 'Value' ]
#      if res:
#        res = S_OK( res[ 0 ][ 0 ] )
#      else:
#        res = S_OK( None )  
#
#    else:
#      res = self.returnERROR( res )
##    except Exception, e:
##      _msg = '%s (%s): %s' % ( self.__class__.__name__, self.args, e )
##      gLogger.exception( _msg )
##      return S_ERROR( _msg )
#
#    return res 

#  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF