""" Class that contains client access to the PilotsLogging handler. """

__RCSID__ = "$Id$"

from DIRAC.Core.Base.Client                         import Client

class PilotsLoggingClient( Client ):

  def __init__( self, **kwargs ):
    Client.__init__( self, **kwargs )
    self.setServer( 'WorkloadManagement/PilotsLogging' )
    self.pilotsLoggingHandler = self._getRPC()

  def addPilotsLogging( self, pilotRef, status, minorStatus, timeStamp, source ):

    return self.pilotsLoggingHandler.addPilotsLogging(pilotRef, status, minorStatus, timeStamp, source)

  def deletePilotsLogging( self, pilotRef ):

    return self.pilotsLoggingHandler.detelePilotsLogging( pilotRef )

  def getPilotsLogging( self, pilotRef ):

    return self.pilotsLoggingHandler.getPilotsLogging( pilotRef )
