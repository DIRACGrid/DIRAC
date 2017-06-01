""" PilotsLoggingHandler is the implementation of the PilotsLogging service

    The following methods are available in the Service interface

    addPilotsLogging()
    getPilotsLogging
    setPilotsUUIDtoIDMapping()
    addPilotsUUID()
    deletePilotsLogging()

"""

__RCSID__ = "$Id: $"

from DIRAC.WorkloadManagementSystem.DB.PilotsLoggingDB import PilotsLoggingDB

from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import S_OK

class PilotsLoggingHandler( RequestHandler ):
  """Server side functions for Pilots Logging service"""

  @classmethod
  def initializeHandler( cls, serviceInfo ):
    """Initialization of Pilots Logging service
    """
    return S_OK()

  def initialize(self):
    """Initialization of Pilots Logging service
    """
    self.pilotsLogging = PilotsLoggingDB()

  types_addPilotsLogging = [ basestring, basestring, basestring, basestring, basestring, basestring ]
  def export_addPilotsLogging( self, pilotUUID, timestamp, source, phase, status, messageContent ):
    """
    Add new Pilots Logging entry
    :param pilotUUID: Pilot reference
    :param status: Pilot status
    :param minorStatus: Additional status information
    :param timeStamp: Date and time of status event
    :param source: Source of statu information
    """

    return self.pilotsLogging.addPilotsLogging( pilotUUID, timestamp, source, phase, status, messageContent )

  types_getPilotsLogging = [ basestring ]
  def export_getPilotsLogging( self, pilotUUID ):
    """
    Get all Logging entries for Pilot
    :param pilotUUID: Pilot reference
    """

    return self.pilotsLogging.getPilotsLogging( pilotUUID )

  types_deletePilotsLogging = [ [basestring,  list] ]
  def export_deletePilotsLogging( self, pilotUUID ):
    """
    Delete all Logging entries for Pilot
    :param pilotUUID: Pilot reference
    """

    return self.pilotsLogging.deletePilotsLogging( pilotUUID )
