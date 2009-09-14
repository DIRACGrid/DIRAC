########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/DataManagementSystem/Agent/BookkeepingWatchAgent.py,v 1.3 2009/09/14 20:36:03 acsmith Exp $
########################################################################

__RCSID__ = "$Id: BookkeepingWatchAgent.py,v 1.3 2009/09/14 20:36:03 acsmith Exp $"

from DIRAC                                            import S_OK, S_ERROR, gConfig, gLogger, gMonitor
from DIRAC.LHCbSystem.Agent.BookkeepingWatchAgent     import BookkeepingWatchAgent


AGENT_NAME = 'DataManagement/BookkeepingWatchAgent'

class BookkeepingWatchAgent(BookkeepingWatchAgent):
  pass
