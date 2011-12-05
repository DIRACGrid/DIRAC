################################################################################
# $HeadURL $
################################################################################
__RCSID__ = "$Id:  $"

"""
  TOWRITE
"""

from DIRAC                                            import gLogger

from DIRAC.ResourceStatusSystem.Command.Command       import Command
from DIRAC.ResourceStatusSystem.Command.knownAPIs     import initAPIs
from DIRAC.ResourceStatusSystem.Utilities.Exceptions  import InvalidRes
from DIRAC.ResourceStatusSystem.Utilities             import Utils
from DIRAC.ResourceStatusSystem                       import ValidRes

################################################################################
################################################################################

class RSPeriods_Command( Command ):

  __APIs__ = [ 'ResourceStatusClient' ]

  def doCommand( self ):
    """
    Return getPeriods from ResourceStatus Client

    - args[0] should be a ValidRes

    - args[1] should be the name of the ValidRes

    - args[2] should be the present status

    - args[3] are the number of hours requested
    """

    super( RSPeriods_Command, self ).doCommand()
    self.APIs = initAPIs( self.__APIs__, self.APIs )

    try:
      res = self.APIs[ 'ResourceStatusClient' ].getPeriods( self.args[0], self.args[1], self.args[2], self.args[3] )['Value']
    except:
      gLogger.exception( "Exception when calling ResourceStatusClient for %s %s" % ( self.args[0], self.args[1] ) )
      return {'Result':'Unknown'}

    return {'Result':res}

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

################################################################################
################################################################################

class ServiceStats_Command( Command ):
  """
  The ServiceStats_Command class is a command class to know about
  present services stats
  """

  __APIs__ = [ 'ResourceStatusClient' ]

  def doCommand( self ):
    """
    Uses :meth:`DIRAC.ResourceStatusSystem.Client.ResourceStatusClient.getServiceStats`

    :params:
      :attr:`args`: a tuple
        - args[1]: a ValidRes

        - args[0]: should be the name of the Site

    :returns:
      {'Active':xx, 'Probing':yy, 'Banned':zz, 'Total':xyz}
    """

    super( ServiceStats_Command, self ).doCommand()
    self.APIs = initAPIs( self.__APIs__, self.APIs )

    try:
      res = self.APIs[ 'ResourceStatusClient' ].getServiceStats( self.args[1] )#, statusType = None )# self.args[0], self.args[1] )['Value']
    except:
      gLogger.exception( "ServiceStats: Exception when calling ResourceStatusClient for %s %s" % ( self.args[0], self.args[1] ) )
      return {'Result':'Unknown'}

    if not res[ 'OK' ]:
      gLogger.error( "ServiceStats: Error %s returned calling ResourceStatusClient for %s %s" % ( res[ 'Message' ], self.args[0], self.args[1] ) )
      return { 'Result' : None }

    return { 'Result' : res[ 'Value' ] }

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

################################################################################
################################################################################

class ResourceStats_Command( Command ):
  """
  The ResourceStats_Command class is a command class to know about
  present resources stats
  """

  __APIs__ = [ 'ResourceStatusClient' ]

  def doCommand( self ):
    """
    Uses :meth:`DIRAC.ResourceStatusSystem.Client.ResourceStatusClient.getResourceStats`

    :params:
      :attr:`args`: a tuple
        - `args[0]` string, a ValidRes. Should be in ('Site', 'Service')

        - `args[1]` should be the name of the Site or Service

    :returns:

    """

    super( ResourceStats_Command, self ).doCommand()
    self.APIs = initAPIs( self.__APIs__, self.APIs )

    try:
      res = self.APIs[ 'ResourceStatusClient' ].getResourceStats( self.args[0], self.args[1], statusType = None )
    except:
      gLogger.exception( "ResourceStats: Exception when calling ResourceStatusClient for %s %s" % ( self.args[0], self.args[1] ) )
      return {'Result':'Unknown'}

    if not res[ 'OK' ]:
      gLogger.error( "ResourceStats: Error %s returned calling ResourceStatusClient for %s %s" % ( res[ 'Message' ], self.args[0], self.args[1] ) )
      return { 'Result' : None }

    return { 'Result' : res[ 'Value' ] }

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

################################################################################
################################################################################

class StorageElementsStats_Command( Command ):
  """
  The StorageElementsStats_Command class is a command class to know about
  present storageElementss stats
  """

  __APIs__ = [ 'ResourceStatusClient' ]

  def doCommand( self ):
    """
    Uses :meth:`DIRAC.ResourceStatusSystem.Client.ResourceStatusClient.getStorageElementStats`

    :params:
      :attr:`args`: a tuple
        - `args[0]` should be in ['Site', 'Resource']

        - `args[1]` should be the name of the Site or Resource

    :returns:

    """

    super( StorageElementsStats_Command, self ).doCommand()
    self.APIs = initAPIs( self.__APIs__, self.APIs )

    if self.args[0] == 'Service':
      granularity = 'Site'
      name        = self.args[1].split( '@' )[1]
    elif self.args[0] in [ 'Site', 'Resource' ]:
      granularity = self.args[0]
      name        = self.args[1]
    else:
      raise InvalidRes, Utils.where( self, self.doCommand )

    try:
      res = self.APIs[ 'ResourceStatusClient' ].getStorageElementStats( granularity, name, statusType = None )
    except:
      gLogger.exception( "StorageElementsStats: Exception when calling ResourceStatusClient for %s %s" % ( granularity, name ) )
      return {'Result':'Unknown'}

    if not res[ 'OK' ]:
      gLogger.error( "StorageElementsStats: Error %s returned calling ResourceStatusClient for %s %s" % ( res[ 'Message' ], granularity, name ) )
      return { 'Result' : None }

    return { 'Result' : res[ 'Value' ] }

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

################################################################################
################################################################################

class MonitoredStatus_Command( Command ):
  """
  The MonitoredStatus_Command class is a command class to know about
  monitored status.
  """

  __APIs__ = [ 'ResourceStatusClient' ]

  def doCommand( self ):
    """
    Uses :meth:`DIRAC.ResourceStatusSystem.Client.ResourceStatusClient.getMonitoredStatus`

    :params:
      :attr:`args`: a tuple
        - `args[0]`: string - should be a ValidRes

        - `args[1]`: string - should be the name of the ValidRes

        - `args[2]`: optional string - a ValidRes (get status of THIS ValidRes
          for name in args[1], will call getGeneralName)

    :returns:
      {'MonitoredStatus': 'Active'|'Probing'|'Banned'}
    """

    super( MonitoredStatus_Command, self ).doCommand()
    self.APIs = initAPIs( self.__APIs__, self.APIs )

    try:
      if len( self.args ) == 3:
        if ValidRes.index( self.args[2] ) >= ValidRes.index( self.args[0] ):
          raise InvalidRes, Utils.where( self, self.doCommand )
        toBeFound = Utils.unpack(self.APIs[ 'ResourceStatusClient' ].getGeneralName( self.args[0], self.args[1], self.args[2] ))[0]

      else:
        toBeFound = self.args[1]

      statuses  = Utils.unpack(self.APIs[ 'ResourceStatusClient' ].getMonitoredStatus( self.args[0], toBeFound ))[0]

    except InvalidRes:
      gLogger.exception( "Exception when calling ResourceStatusClient for %s %s" % ( self.args[0], self.args[1] ) )
      return {'Result':'Unknown'}
    except IndexError:
      gLogger.exception( "IndexError for %s %s" % ( self.args[0], self.args[1] ) )
      return {'Result':'Unknown'}

    return {'Result':statuses[0]}

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
