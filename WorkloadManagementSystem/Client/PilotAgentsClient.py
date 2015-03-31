""" Class that contains client access to the PilotAgents handler. """
########################################################################
# $Id$
# $HeadURL$
########################################################################
__RCSID__ = "$Id$"

from DIRAC.Core.Base.Client                         import Client

class PilotAgentsClient( Client ):
  
  def __init__( self, **kwargs ):
    Client.__init__( self, **kwargs )
    self.setServer( 'WorkloadManagement/PilotAgents' )
    self.pilotAgenstHandler = self._getRPC()
    
  def addPilotsLogging( self, pilotUUID, status, minorStatus, timeStamp, source, pilotID ):
    
    resp = self.pilotAgenstHandler.addPilotsUUID( pilotUUID )
    if not resp['OK']:
      return resp
    
    if pilotID:
      resp = self.pilotAgenstHandler.setPilotsUUIDtoIDMapping( pilotUUID, pilotID )
      if not resp['OK']:
        return resp
    
    return self.pilotAgenstHandler.addPilotsLogging(pilotUUID, status, minorStatus, timeStamp, source)
  
  def setPilotsUUIDtoIDMapping( self, pilotUUID, pilotID ):
    
    return self.pilotAgenstHandler.setPilotsUUIDtoIDMapping( pilotUUID, pilotID )
  
  def deletePilotsLogging( self, pilotID ):
    
    return self.pilotAgenstHandler.detelePilotsLogging( pilotID )
  
  def getPilotsLogging( self, pilotID ):
    
    return self.pilotAgenstHandler.getPilotsLogging( pilotID )