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
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities import Time

class PilotsLoggingHandler( RequestHandler ):

  @classmethod
  def initializeHandler( cls, serviceInfo ):
    return S_OK()

  def initialize(self):
    self.pilotsLogging = PilotsLoggingDB()

  types_addPilotsLogging = [ basestring, basestring, basestring, float, basestring ]
  def export_addPilotsLogging( self, pilotUUID, status, minorStatus, timeStamp, source ):

    return self.pilotsLogging.addPilotsLogging( pilotUUID, status, minorStatus, timeStamp, source )

  types_getPilotsLogging = [ [int, long] ]
  def export_getPilotsLogging( self, pilotID ):

    return self.pilotsLogging.getPilotsLogging( pilotID )

  types_setPilotsUUIDtoIDMapping = [ basestring, [int, long] ]
  def export_setPilotsUUIDtoIDMapping( self, pilotUUID, pilotID ):

    return self.pilotsLogging.setPilotsUUIDtoIDMapping( pilotUUID, pilotID )

  types_addPilotsUUID = [ basestring ]
  def export_addPilotsUUID(self, pilotUUID ):

    return self.pilotsLogging.addPilotsUUID( pilotUUID )

  types_deletePilotsLogging = [ [int, long,  list] ]
  def export_deletePilotsLogging( self, pilotID ):

    return self.pilotsLogging.deletePilotsLogging( pilotID )
