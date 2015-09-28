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

import types
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities import Time
# from DIRAC.WorkloadManagementSystem.DB import PilotAgentsDB

class PilotsLoggingHandler( RequestHandler ):

  @classmethod
  def initializeHandler( cls, serviceInfo ):
    return S_OK()

  def initialize(self):
    self.pilotsLogging = PilotsLoggingDB()

  auth_addPilotsLogging = [ 'all' ]
  types_addPilotsLogging = [ types.StringType, types.StringType, types.StringType, types.FloatType, types.StringType ]
  def export_addPilotsLogging( self, pilotUUID, status, minorStatus, timeStamp, source ):

    return self.pilotsLogging.addPilotsLogging( pilotUUID, status, minorStatus, timeStamp, source )

  auth_getPilotsLogging = [ 'all' ]
  types_getPilotsLogging = [ types.IntType ]
  def export_getPilotsLogging( self, pilotID ):

    return self.pilotsLogging.getPilotsLogging( pilotID )

  auth_setPilotsUUIDtoIDMapping = [ 'all' ]
  types_setPilotsUUIDtoIDMapping = [ types.StringType, types.IntType ]
  def export_setPilotsUUIDtoIDMapping( self, pilotUUID, pilotID ):

    return self.pilotsLogging.setPilotsUUIDtoIDMapping( pilotUUID, pilotID )

  auth_addPilotsUUID = [ 'all' ]
  types_addPilotsUUID = [ types.StringType ]
  def export_addPilotsUUID(self, pilotUUID ):

    return self.pilotsLogging.addPilotsUUID( pilotUUID )

  auth_detelePilotsLogging = [ 'all' ]
  types_detelePilotsLogging = [ types.IntType ]
  def export_detelePilotsLogging( self, pilotID ):

    return self.pilotsLogging.deletePilotsLogging( pilotID )
