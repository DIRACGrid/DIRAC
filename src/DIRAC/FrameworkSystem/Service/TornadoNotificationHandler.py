""" TornadoNotification is the implementation of the Notification service in HTTPS

    .. literalinclude:: ../ConfigTemplate.cfg
      :start-after: ##BEGIN TornadoNotification:
      :end-before: ##END
      :dedent: 2
      :caption: TornadoNotification options
"""
from DIRAC.Core.Tornado.Server.TornadoService import TornadoService
from DIRAC.FrameworkSystem.Service.NotificationHandler import NotificationHandlerMixin


class TornadoNotificationHandler(NotificationHandlerMixin, TornadoService):
    pass
