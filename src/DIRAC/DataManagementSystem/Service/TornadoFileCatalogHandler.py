########################################################################
# File: FileCatalogHandler.py
########################################################################
"""
:mod: FileCatalogHandler

.. module: FileCatalogHandler

:synopsis: FileCatalogHandler is a simple Replica and Metadata Catalog service

"""
# imports
import json
import csv

from io import StringIO

# from DIRAC

from DIRAC import gLogger, S_ERROR
from DIRAC.Core.Utilities.ReturnValues import returnValueOrRaise
from DIRAC.DataManagementSystem.Service.FileCatalogHandler import FileCatalogHandlerMixin

from DIRAC.Core.Tornado.Server.TornadoService import TornadoService


sLog = gLogger.getSubLogger(__name__)


class TornadoFileCatalogHandler(FileCatalogHandlerMixin, TornadoService):
    """
    ..class:: FileCatalogHandler

    A simple Replica and Metadata Catalog service.
    """

    # This is needed because the mixin class uses `cls.log`
    log = sLog

    def export_streamToClient(self, jsonSENames):
        """This method is used to transfer the SEDump to the client,
        formated as CSV with '|' separation

        :param jsonSENames: json formated names of the SEs to dump

        :returns: the result of the FileHelper


        """
        seNames = json.loads(jsonSENames)
        csvOutput = None

        try:
            retVal = returnValueOrRaise(self.getSEDump(seNames))

            csvOutput = StringIO()
            writer = csv.writer(csvOutput, delimiter="|")
            writer.writerows(retVal)

            ret = csvOutput.getvalue()
            return ret

        except Exception as e:
            sLog.exception("Exception while sending seDump", repr(e))
            return S_ERROR("Exception while sendind seDump: %s" % repr(e))
        finally:
            if csvOutput is not None:
                csvOutput.close()
