import time

from tornado import gen

# We change the import name otherwise sphinx tries
# to compile the tornado doc and fails
from tornado.ioloop import IOLoop as _IOLoop

from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
from DIRAC.ConfigurationSystem.private.RefresherBase import RefresherBase
from DIRAC.FrameworkSystem.Client.Logger import gLogger


class TornadoRefresher(RefresherBase):
    """
    The refresher, modified for Tornado
    It's the same refresher, the only thing which change is
    that we are using the IOLoop instead of threads for background
    tasks, so it work with Tornado (HTTPS server).
    """

    def refreshConfigurationIfNeeded(self):
        """
        Trigger an automatic refresh, most of the time nothing happens because automaticUpdate is enabled.
        This function is called by gConfig.getValue most of the time.

        We disable pylint error because this class must be instanciated by a mixin to define the missing methods
        """
        if not self._refreshEnabled or self._automaticUpdate:  # pylint: disable=no-member
            return
        if not gConfigurationData.getServers() or not self._lastRefreshExpired():  # pylint: disable=no-member
            return
        self._lastUpdateTime = time.time()
        _IOLoop.current().run_in_executor(None, self._refresh)  # pylint: disable=no-member

    def autoRefreshAndPublish(self, sURL):
        """
        Start the autorefresh background task, called by ServiceInterface
        (the class behind the Configuration/Server handler)

        :param str sURL: URL of the configuration server
        """
        gLogger.debug("Setting configuration refresh as automatic")
        if not gConfigurationData.getAutoPublish():
            gLogger.debug("Worker server won't auto publish itself")
        if not gConfigurationData.getName():
            import DIRAC

            DIRAC.abort(10, "Missing configuration name!")
        self._url = sURL
        self._automaticUpdate = True

        # Tornado replacement solution to the classic thread
        # It start the method self.__refreshLoop on the next IOLoop iteration
        _IOLoop.current().spawn_callback(self.__refreshLoop)

    @gen.coroutine
    def __refreshLoop(self):
        """
        Trigger the autorefresh when configuration is expired

        This task must use Tornado utilities to avoid blocking the ioloop and
        potentialy deadlock the server.

        See http://www.tornadoweb.org/en/stable/guide/coroutines.html#looping
        for official documentation about this type of method.
        """
        while self._automaticUpdate:
            # This is the sleep from Tornado, like a sleep it wait some time
            # But this version is non-blocking, so IOLoop can continue execution
            yield gen.sleep(gConfigurationData.getPropagationTime())
            # Publish step is blocking so we have to run it in executor
            # If we are not doing it, when controller try to ping we block the IOLoop

            yield _IOLoop.current().run_in_executor(None, self.__AutoRefresh)

    def __AutoRefresh(self):
        """
        Auto refresh the configuration
        We disable pylint error because this class must be instanciated
        by a mixin to define the methods.
        """
        if self._refreshEnabled:  # pylint: disable=no-member
            if not self._refreshAndPublish():  # pylint: disable=no-member
                gLogger.error("Can't refresh configuration from any source")

    def daemonize(self):
        """daemonize is probably not the best name because there is no daemon behind
        but we must keep it to the same interface of the DISET refresher"""
        _IOLoop.current().spawn_callback(self.__refreshLoop)
