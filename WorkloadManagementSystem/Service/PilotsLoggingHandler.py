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
  def export_addPilotsLogging( self, pilotRef, status, minorStatus, timeStamp, source ):

    return self.pilotsLogging.addPilotsLogging( pilotRef, status, minorStatus, timeStamp, source )

  types_getPilotsLogging = [ basestring ]
  def export_getPilotsLogging( self, pilotRef ):

    return self.pilotsLogging.getPilotsLogging( pilotRef )

  types_deletePilotsLogging = [ [basestring,  list] ]
  def export_deletePilotsLogging( self, pilotRef ):

    return self.pilotsLogging.deletePilotsLogging( pilotRef )
