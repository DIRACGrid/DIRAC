""" Hello Service is an example of how to build services in the DIRAC framework
"""

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Tornado.Server.TornadoService import TornadoService

sLog = gLogger.getSubLogger(__name__)


class HelloHandler(TornadoService):
    @classmethod
    def initializeHandler(cls, serviceInfo):
        """Handler initialization"""
        cls.defaultWhom = "World"
        return S_OK()

    def initialize(self):
        """Response initialization"""
        self.requestDefaultWhom = self.srv_getCSOption("DefaultWhom", HelloHandler.defaultWhom)

    auth_sayHello = ["all"]
    types_sayHello = [str]

    def export_sayHello(self, whom):
        """Say hello to somebody"""

        sLog.notice("Called sayHello of HelloHandler with whom", whom)

        if not whom:
            whom = self.requestDefaultWhom

        # Create a local logger which will always contain
        # the whom parameter
        log = sLog.getLocalSubLogger(whom)

        if whom.lower() == "nobody":
            log.notice("Mummy !!! The weird guy over there offered me candies !")
            return S_ERROR("Not greeting anybody!")

        log.notice("It's okay to say hello")

        return S_OK("Hello " + whom)
