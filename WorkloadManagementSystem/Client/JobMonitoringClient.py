""" Class that contains client access to the job monitoring handler. """
########################################################################
# $Id$
# $HeadURL$
########################################################################
__RCSID__ = "$Id$"

from DIRAC.Core.Base.Client                         import Client

class JobMonitoringClient( Client ):

  def __init__( self, **kwargs ):

    Client.__init__( self, **kwargs )
    self.setServer( 'WorkloadManagement/JobMonitoring' )
    self.monitoringHandler = self._getRPC()

  def traceJobParameters( self, site, localID, parameterList = None, date = None ):
    return self.monitoringHandler.traceJobParameters( site, localID, parameterList, date )

  def traceJobParameter( self, site, localID, parameter, date = None ):
    return self.monitoringHandler.traceJobParameter( site, localID, parameter, date )
