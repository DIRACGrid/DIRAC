########################################################################
# $HeadURL$
# File :   PilotMonitor.py
# Author : Stuart Paterson
########################################################################
"""  The Pilot Monitor Agent controls the tracking of pilots via the AgentMonitor and Grid
     specific sub-classes. This is a simple wrapper that performs the instantiation and monitoring
     of the AgentMonitor instance for all Grids.
"""

__RCSID__ = "$Id$"

from DIRAC.Core.Base.AgentModule    import AgentModule
from DIRAC                          import S_OK
from DIRAC.WorkloadManagementSystem.DB.PilotAgentsDB import PilotAgentsDB

class PilotMonitorAgent( AgentModule ):
  """
      The specific agents must provide the following methods:
      - initialize() for initial settings
      - beginExecution()
      - execute() - the main method called in the agent cycle
      - endExecution()
      - finalize() - the graceful exit of the method, this one is usually used
                 for the agent restart
  """

  #############################################################################
  def initialize( self ):
    """Sets defaults
    """
    self.am_setOption( 'PollingTime', 120 )
    self.clearPilotsDelay = self.am_getOption( 'ClearPilotsDelay', 30 )
    self.clearAbortedDelay = self.am_getOption( 'ClearAbortedPilotsDelay', 7 )

    self.pilotDB = PilotAgentsDB()
    return S_OK()

  #############################################################################
  def execute( self ):
    """
      Remove from PilotDB pilots that:
      - are older than self.clearPilotsDelay
      - are Aborted and older than self.clearAbortedDelay
    """
    result = self.pilotDB.clearPilots( self.clearPilotsDelay, self.clearAbortedDelay )
    if not result['OK']:
      self.log.warn( 'Failed to clear old pilots in the PilotAgentsDB' )

    return S_OK( 'Monitoring cycle complete.' )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
