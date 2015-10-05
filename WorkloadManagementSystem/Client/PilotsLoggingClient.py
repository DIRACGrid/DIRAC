""" Class that contains client access to the PilotsLogging handler. """
########################################################################
# $Id$
# $HeadURL$
########################################################################
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

  def addPilotsLogging( self, pilotRef, status, minorStatus, timeStamp, source ):
    """
    Add new Pilots Logging entry
    :param pilotRef: Pilot reference
    :param status: Pilot status
    :param minorStatus: Additional status information
    :param timeStamp: Date and time of status event
    :param source: Source of statu information
    """

    return self.pilotsLoggingHandler.addPilotsLogging(pilotRef, status, minorStatus, timeStamp, source)

  def deletePilotsLogging( self, pilotRef ):
    """
    Delete all Logging entries for Pilot
    :param pilotRef: Pilot reference
    """

    return self.pilotsLoggingHandler.detelePilotsLogging( pilotRef )

  def getPilotsLogging( self, pilotRef ):
    """
    Get all Logging entries for Pilot
    :param pilotRef: Pilot reference
    """

    return self.pilotsLoggingHandler.getPilotsLogging( pilotRef )
