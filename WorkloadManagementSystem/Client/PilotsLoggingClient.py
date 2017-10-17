""" Class that contains client access to the PilotsLogging handler. """

__RCSID__ = "$Id$"

from DIRAC.Core.Base.Client import Client

class PilotsLoggingClient( Client ):
  """Implementation of interface of Pilots Logging service. Client class should be used to communicate
  with PilotsLogging Service
  """

  def __init__( self, **kwargs ):
    Client.__init__( self, **kwargs )
    self.setServer( 'WorkloadManagement/PilotsLogging' )
    self.pilotsLoggingHandler = self._getRPC()

  def addPilotsLogging( self, pilotUUID, timestamp, source, phase, status, messageContent ):
    """
    Add new Pilots Logging entry

    :param pilotUUID: Pilot reference
    :param status: Pilot status
    :param minorStatus: Additional status information
    :param timestamp: Date and time of status event
    :param source: Source of status information
    """

    return self.pilotsLoggingHandler.addPilotsLogging(pilotUUID, timestamp, source, phase, status, messageContent)

  def deletePilotsLogging( self, pilotUUID ):
    """
    Delete all Logging entries for Pilot

    :param pilotUUID: Pilot reference
    """

    return self.pilotsLoggingHandler.deletePilotsLogging( pilotUUID )

  def getPilotsLogging( self, pilotUUID ):
    """
    Get all Logging entries for Pilot
    
    :param pilotUUID: Pilot reference
    """

    return self.pilotsLoggingHandler.getPilotsLogging( pilotUUID )
