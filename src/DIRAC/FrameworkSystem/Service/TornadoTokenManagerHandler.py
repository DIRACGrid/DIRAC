"""TornadoTokenManager service is a HTTPs-exposed service responsible for token management, namely storing, updating,
requesting new tokens for DIRAC components that have the appropriate permissions.

.. literalinclude:: ../ConfigTemplate.cfg
    :start-after: ##BEGIN TokenManager:
    :end-before: ##END
    :dedent: 2
    :caption: TokenManager options

The most common use of this service is to obtain tokens with certain scope to return to the user for its purposes,
or to provide to the DIRAC service to perform asynchronous tasks on behalf of the user.
This is mainly about the :py:meth:`export_getToken` method.

.. image:: /_static/Systems/FS/TokenManager_getToken.png
    :alt: https://dirac.readthedocs.io/en/integration/_images/TokenManager_getToken.png (source https://github.com/TaykYoku/DIRACIMGS/raw/main/TokenManagerService_getToken.ai)

The client has a mechanism for caching the received tokens.
This helps reducing the number of requests to both the service and the Identity Provider (IdP).

If the client has a valid **access token** in the cache, it is used until it expires.
After that you need to update. The client can update it independently if on the server where it is in ``dirac.cfg``
``client_id`` and ``client_secret`` of the Identity Provider client are registered.

Otherwise, the client makes an RPC call to the **TornadoManager** service.
The ``refresh token`` from :py:class:`TokenDB <DIRAC.FrameworkSystem.DB.TokenDB.TokenDB>`
is taken and the **exchange token** request to Identity Provider is made.
"""

from DIRAC.Core.Tornado.Server.TornadoService import TornadoService
from DIRAC.FrameworkSystem.Service.TokenManagerHandler import TokenManagerHandlerMixin


class TornadoTokenManagerHandler(TokenManagerHandlerMixin, TornadoService):
    pass
