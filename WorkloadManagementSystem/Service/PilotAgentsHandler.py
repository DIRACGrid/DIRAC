""" Hello Service is an example of how to build services in the DIRAC framework
"""
from DIRAC.WorkloadManagementSystem.DB.PilotAgentsDB import PilotAgentsDB

__RCSID__ = "$Id: $"

import types
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities import Time
# from DIRAC.WorkloadManagementSystem.DB import PilotAgentsDB

class PilotAgentsHandler( RequestHandler ):
  
  @classmethod
  def initializeHandler( cls, serviceInfo ):
    return S_OK()
  
  def initialize(self):
    self.pilotAgents = PilotAgentsDB()
    
  auth_addPilotsLogging = [ 'all' ]
  types_addPilotsLogging = [ types.StringType, types.StringType, types.StringType, types.FloatType, types.StringType ]
  def export_addPilotsLogging( self, pilotUUID, status, minorStatus, timeStamp, source ):
    
    return self.pilotAgents.addPilotsLogging( pilotUUID, status, minorStatus, timeStamp, source )
  
  auth_getPilotsLogging = [ 'all' ]
  types_getPilotsLogging = [ types.IntType ]
  def export_getPilotsLogging( self, pilotID ):
    
    return self.pilotAgents.getPilotsLogging( pilotID )
  
  auth_setPilotsUUIDtoIDMapping = [ 'all' ]
  types_setPilotsUUIDtoIDMapping = [ types.StringType, types.IntType ]
  def export_setPilotsUUIDtoIDMapping( self, pilotUUID, pilotID ):
    
    return self.pilotAgents.setPilotsUUIDtoIDMapping( pilotUUID, pilotID )

  auth_addPilotsUUID = [ 'all' ]
  types_addPilotsUUID = [ types.StringType ]
  def export_addPilotsUUID(self, pilotUUID ):
    
    return self.pilotAgents.addPilotsUUID( pilotUUID )

  auth_detelePilotsLogging = [ 'all' ]
  types_detelePilotsLogging = [ types.IntType ]
  def export_detelePilotsLogging( self, pilotID ):
    
    return self.pilotAgents.deletePilotsLogging( pilotID )