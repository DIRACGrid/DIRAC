""" Tornado-based HTTPs JobStateUpdate service.
"""


from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

from DIRAC import gLogger
from DIRAC.Core.Tornado.Server.TornadoService import TornadoService
from DIRAC.WorkloadManagementSystem.Service.JobStateUpdateHandler import JobStateUpdateHandlerMixin


sLog = gLogger.getSubLogger(__name__)


class TornadoJobStateUpdateHandler(JobStateUpdateHandlerMixin, TornadoService):
    log = sLog
