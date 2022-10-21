""" TornadoUserProfileManager is the implementation of the UserProfileManager service in HTTPS

    .. literalinclude:: ../ConfigTemplate.cfg
      :start-after: ##BEGIN TornadoUserProfileManager:
      :end-before: ##END
      :dedent: 2
      :caption: TornadoUserProfileManager options
"""
from DIRAC.Core.Tornado.Server.TornadoService import TornadoService
from DIRAC.FrameworkSystem.Service.UserProfileManagerHandler import UserProfileManagerHandlerMixin


class TornadoUserProfileManagerHandler(UserProfileManagerHandlerMixin, TornadoService):
    pass
