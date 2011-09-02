""" The Pilots_Command class is a command class to know about
    present pilots efficiency
"""

from DIRAC                                            import gLogger

from DIRAC.ResourceStatusSystem.Command.Command       import Command
from DIRAC.ResourceStatusSystem.Utilities.Exceptions  import InvalidRes
from DIRAC.ResourceStatusSystem.Utilities.Utils       import where
from DIRAC.ResourceStatusSystem                       import ValidRes, ValidStatus
from DIRAC.ResourceStatusSystem.PolicySystem.Status   import value_of_status

#############################################################################

class RSPeriods_Command( Command ):

  def doCommand( self ):
    """
    Return getPeriods from ResourceStatus Client

    - args[0] should be a ValidRes

    - args[1] should be the name of the ValidRes

    - args[2] should be the present status

    - args[3] are the number of hours requested
    """
    super( RSPeriods_Command, self ).doCommand()

    if self.client is None:
      from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
      self.client = ResourceStatusClient()

    try:
      res = self.client.getPeriods( self.args[0], self.args[1], self.args[2], self.args[3] )['Value']
    except:
      gLogger.exception( "Exception when calling ResourceStatusClient for %s %s" % ( self.args[0], self.args[1] ) )
      return {'Result':'Unknown'}

    return {'Result':res}

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

#############################################################################

class ServiceStats_Command( Command ):
  """
  The ServiceStats_Command class is a command class to know about
  present services stats
  """

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

    if self.client is None:
      from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
      self.client = ResourceStatusClient( timeout = self.timeout )

    try:
      res = self.client.getServiceStats( self.args[0], self.args[1] )['Value']
    except:
      gLogger.exception( "Exception when calling ResourceStatusClient for %s %s" % ( self.args[0], self.args[1] ) )
      return {'Result':'Unknown'}

    return {'Result':res}

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

#############################################################################

class ResourceStats_Command( Command ):
  """
  The ResourceStats_Command class is a command class to know about
  present resources stats
  """

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

    if self.client is None:
      from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
      self.client = ResourceStatusClient( timeout = self.timeout )

    try:
      res = self.client.getResourceStats( self.args[0], self.args[1] )['Value']
    except:
      gLogger.exception( "Exception when calling ResourceStatusClient for %s %s" % ( self.args[0], self.args[1] ) )
      return {'Result':'Unknown'}

    return {'Result':res}

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

#############################################################################

class StorageElementsStats_Command( Command ):
  """
  The StorageElementsStats_Command class is a command class to know about
  present storageElementss stats
  """

  def doCommand( self ):
    """
    Uses :meth:`DIRAC.ResourceStatusSystem.Client.ResourceStatusClient.getStorageElementsStats`

    :params:
      :attr:`args`: a tuple
        - `args[0]` should be in ['Site', 'Resource']

        - `args[1]` should be the name of the Site or Resource

    :returns:

    """
    super( StorageElementsStats_Command, self ).doCommand()

    if self.args[0] in ( 'Service', 'Services' ):
      granularity = 'Site'
      name = self.args[1].split( '@' )[1]
    elif self.args[0] in ( 'Site', 'Sites', 'Resource', 'Resources' ):
      granularity = self.args[0]
      name = self.args[1]
    else:
      raise InvalidRes, where( self, self.doCommand )

    if self.client is None:
      from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
      self.client = ResourceStatusClient( timeout = self.timeout )

    try:
      resR = self.client.getStorageElementsStats( granularity, name, 'Read' )['Value']
      resW = self.client.getStorageElementsStats( granularity, name, 'Write' )['Value']
    except:
      gLogger.exception( "Exception when calling ResourceStatusClient for %s %s" % ( granularity, name ) )
      return {'Result':'Unknown'}

    res = {}
    for key in ValidStatus:
      res[ key ] = resR[ key ] + resW[ key ]

    return {'Result':res}

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

#############################################################################

class MonitoredStatus_Command( Command ):
  """
  The MonitoredStatus_Command class is a command class to know about
  monitored status.
  """

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

    if self.client is None:
      from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
      self.client = ResourceStatusClient( timeout = self.timeout )

    try:
      if len( self.args ) == 3:
        if ValidRes.index( self.args[2] ) >= ValidRes.index( self.args[0] ):
          raise InvalidRes, where( self, self.doCommand )

        toBeFound = self.client.getGeneralName( self.args[0], self.args[1], self.args[2] )
        if not toBeFound[ 'OK' ]:
          return {'Result' : 'Unknown'}
        toBeFound = toBeFound['Value']

        statuses = self.client.getMonitoredStatus( self.args[2], toBeFound )
        if not statuses['OK']:
          return {'Result' : 'Unknown'}
        statuses = statuses['Value']

      else:
        toBeFound = self.args[1]
        statuses = self.client.getMonitoredStatus( self.args[0], toBeFound )

        if not statuses['OK']:
          return {'Result' : 'Unknown'}
        statuses = statuses['Value']

      if not statuses:
        gLogger.warn( "No status found for %s" % toBeFound )
        return {'Result':'Unknown'}

    except:
      gLogger.exception( "Exception when calling ResourceStatusClient for %s %s" % ( self.args[0], self.args[1] ) )
      return {'Result':'Unknown'}

    # statuses is a list of statuses. We take the worst returned
    # status.

    assert(type(statuses) == list)
    statuses.sort(key=value_of_status)

    res = statuses[0]

    if len(statuses) > 1:
      gLogger.info( ValidStatus )
      gLogger.info( statuses )

    return {'Result':res}


  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

#############################################################################
