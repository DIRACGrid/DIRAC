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

  types_addPilotsLogging = [ basestring, basestring, basestring, float, basestring ]
  def export_addPilotsLogging( self, pilotRef, status, minorStatus, timeStamp, source ):
    """
    Add new Pilots Logging entry
    :param pilotRef: Pilot reference
    :param status: Pilot status
    :param minorStatus: Additional status information
    :param timeStamp: Date and time of status event
    :param source: Source of statu information
    """

    return self.pilotsLogging.addPilotsLogging( pilotRef, status, minorStatus, timeStamp, source )

  types_getPilotsLogging = [ basestring ]
  def export_getPilotsLogging( self, pilotRef ):
    """
    Get all Logging entries for Pilot
    :param pilotRef: Pilot reference
    """

    return self.pilotsLogging.getPilotsLogging( pilotRef )

  types_deletePilotsLogging = [ [basestring,  list] ]
  def export_deletePilotsLogging( self, pilotRef ):
    """
    Delete all Logging entries for Pilot
    :param pilotRef: Pilot reference
    """

    return self.pilotsLogging.deletePilotsLogging( pilotRef )

  types_addPilotsUUID = [ basestring ]
  auth_addPilotsUUID = [ 'Operator' ]
  def export_addPilotsUUID(self, pilotUUID ):

    return self.pilotsLogging.addPilotsUUID( pilotUUID )

  types_detelePilotsLogging = [ types.IntType ]
  auth_detelePilotsLogging = [ 'Operator' ]
  def export_detelePilotsLogging( self, pilotID ):

    return self.pilotsLogging.deletePilotsLogging( pilotID )
