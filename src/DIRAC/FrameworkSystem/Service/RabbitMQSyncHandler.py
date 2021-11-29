""" RabbitMQSync service to synchronize the internal RabbitMQ database.
    according to CS content. The whole work is done by the RabbitMQSynchronizer
    that is activated when the CS was changed.
"""
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import S_OK
from DIRAC import gConfig
from DIRAC.FrameworkSystem.Utilities import RabbitMQSynchronizer


class RabbitMQSyncHandler(RequestHandler):
    """Service to synchronize the content of internal RabbitMQ database
    with the CS content. The work is done by the RabbitMQSynchronizer
    that acts when the CS is changed.
    """

    @classmethod
    def initializeHandler(cls, _serviceInfo):
        """Handler initialization"""
        syncObject = RabbitMQSynchronizer.RabbitMQSynchronizer()
        gConfig.addListenerToNewVersionEvent(syncObject.sync)
        return S_OK()
