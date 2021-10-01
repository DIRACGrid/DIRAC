from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

from DIRAC import gLogger
from DIRAC.Core.Tornado.Server.TornadoService import TornadoService
from DIRAC.DataManagementSystem.Service.FTS3ManagerHandler import FTS3ManagerHandlerMixin

sLog = gLogger.getSubLogger(__name__)


class TornadoFTS3ManagerHandler(FTS3ManagerHandlerMixin, TornadoService):
    """Tornado handler for the FTS3Manager"""

    log = sLog
