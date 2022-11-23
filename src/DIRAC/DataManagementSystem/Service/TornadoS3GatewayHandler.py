""" TornadoS3Gateway is the implementation of the S3Gateway service in HTTPS

    .. literalinclude:: ../ConfigTemplate.cfg
      :start-after: ##BEGIN TornadoS3Gateway
      :end-before: ##END
      :dedent: 2
      :caption: TornadoS3Gateway options
"""
from DIRAC.Core.Tornado.Server.TornadoService import TornadoService
from DIRAC.DataManagementSystem.Service.S3GatewayHandler import S3GatewayHandlerMixin


class TornadoS3GatewayHandler(S3GatewayHandlerMixin, TornadoService):
    pass
