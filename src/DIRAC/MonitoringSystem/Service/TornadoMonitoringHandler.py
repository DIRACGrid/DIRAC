""" Tornado-based HTTPs Monitoring service.

.. literalinclude:: ../ConfigTemplate.cfg
  :start-after: ##BEGIN TornadoMonitoring
  :end-before: ##END
  :dedent: 2
  :caption: Monitoring options

"""

from DIRAC import S_ERROR
from DIRAC.Core.Tornado.Server.TornadoService import TornadoService
from DIRAC.Core.Utilities.Plotting import gDataCache
from DIRAC.Core.Utilities.Plotting.Plots import generateErrorMessagePlot
from DIRAC.MonitoringSystem.Service.MonitoringHandler import MonitoringHandlerMixin


class TornadoMonitoringHandler(MonitoringHandlerMixin, TornadoService):

    types_streamToClient = []

    def export_streamToClient(self, fileId):
        """
        Get graphs data

        :param str fileId: encoded plot attributes
        """

        # First check if we've got to generate the plot
        if len(fileId) > 5 and fileId[1] == ":":
            self.log.info("Seems the file request is a plot generation request!")
            try:
                result = self._generatePlotFromFileId(fileId)
            except Exception as e:  # pylint: disable=broad-except
                self.log.exception("Exception while generating plot", str(e))
                result = S_ERROR(f"Error while generating plot: {str(e)}")
            if not result["OK"]:
                return generateErrorMessagePlot(result["Message"])
            fileId = result["Value"]

        retVal = gDataCache.getPlotData(fileId)
        if not retVal["OK"]:
            return generateErrorMessagePlot(result["Message"])
        return retVal["Value"]
