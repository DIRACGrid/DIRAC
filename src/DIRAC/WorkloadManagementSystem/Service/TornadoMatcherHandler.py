""" Tornado-based HTTPs Matcher service.
"""


from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

from DIRAC import gLogger
from DIRAC.Core.Tornado.Server.TornadoService import TornadoService
from DIRAC.WorkloadManagementSystem.Service.MatcherHandler import MatcherHandlerMixin


sLog = gLogger.getSubLogger(__name__)


class TornadoMatcherHandler(MatcherHandlerMixin, TornadoService):
  log = sLog
