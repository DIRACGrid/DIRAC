""" Class that contains client access to the PilotsLogging handler. """
########################################################################
# $Id$
# $HeadURL$
########################################################################
__RCSID__ = "$Id$"

from DIRAC.Core.Base.Client                         import Client

class PilotsLoggingClient( Client ):

  def __init__( self, **kwargs ):
    Client.__init__( self, **kwargs )
    self.setServer( 'WorkloadManagement/PilotsLogging' )
    self.pilotsLoggingHandler = self._getRPC()

  def addPilotsLogging( self, pilotUUID, status, minorStatus, timeStamp, source, pilotID ):

    resp = self.pilotsLoggingHandler.addPilotsUUID( pilotUUID )
    if not resp['OK']:
      return resp

    if pilotID:
      resp = self.pilotsLoggingHandler.setPilotsUUIDtoIDMapping( pilotUUID, pilotID )
      if not resp['OK']:
        return resp

    return self.pilotsLoggingHandler.addPilotsLogging(pilotUUID, status, minorStatus, timeStamp, source)

  def setPilotsUUIDtoIDMapping( self, pilotUUID, pilotID ):

    return self.pilotsLoggingHandler.setPilotsUUIDtoIDMapping( pilotUUID, pilotID )

  def deletePilotsLogging( self, pilotID ):

    return self.pilotsLoggingHandler.detelePilotsLogging( pilotID )

  def getPilotsLogging( self, pilotID ):

    return self.pilotsLoggingHandler.getPilotsLogging( pilotID )
