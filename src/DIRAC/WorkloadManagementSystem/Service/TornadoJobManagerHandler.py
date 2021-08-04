""" Tornado-based HTTPs JobManager service.
"""


from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

from DIRAC import gLogger
from DIRAC.Core.Tornado.Server.TornadoService import TornadoService
from DIRAC.WorkloadManagementSystem.Service.JobManagerHandler import JobManagerHandlerMixin


sLog = gLogger.getSubLogger(__name__)


class TornadoJobManagerHandler(JobManagerHandlerMixin, TornadoService):
  log = sLog
