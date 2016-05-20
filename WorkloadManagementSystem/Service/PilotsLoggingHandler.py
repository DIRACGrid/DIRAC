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
from DIRAC import S_OK, S_ERROR

class PilotsLoggingHandler( RequestHandler ):

  @classmethod
  def initializeHandler( cls, serviceInfo ):
    return S_OK()

  def initialize(self):
    self.pilotsLogging = PilotsLoggingDB()

  types_addPilotsLogging = [ basestring, basestring, basestring, float, basestring ]
  auth_addPilotsLogging = [ 'Operator' ]
  def export_addPilotsLogging( self, pilotUUID, status, minorStatus, timeStamp, source ):

    return self.pilotsLogging.addPilotsLogging( pilotRef, status, minorStatus, timeStamp, source )

  types_getPilotsLogging = [ [int, long] ]
  auth_getPilotsLogging = [ 'authenticated' ]
  def export_getPilotsLogging( self, pilotID ):

    return self.pilotsLogging.getPilotsLogging( pilotRef )

  types_setPilotsUUIDtoIDMapping = [ basestring, [int, long] ]
  auth_setPilotsUUIDtoIDMapping = [ 'Operator' ]
  def export_setPilotsUUIDtoIDMapping( self, pilotUUID, pilotID ):

    return self.pilotsLogging.deletePilotsLogging( pilotRef )

  types_addPilotsUUID = [ basestring ]
  auth_addPilotsUUID = [ 'Operator' ]
  def export_addPilotsUUID(self, pilotUUID ):

    return self.pilotsLogging.addPilotsUUID( pilotUUID )

  types_deletePilotsLogging = [ [int, long,  list] ]
  def export_deletePilotsLogging( self, pilotID ):

    return self.pilotsLogging.deletePilotsLogging( pilotID )
