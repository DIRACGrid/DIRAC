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

from DIRAC import S_ERROR
from DIRAC.Core.Utilities.ReturnValues import returnValueOrRaise
from DIRAC.DataManagementSystem.Service.FileCatalogHandler import FileCatalogHandlerMixin

from DIRAC.Core.Tornado.Server.TornadoService import TornadoService


class TornadoFileCatalogHandler(FileCatalogHandlerMixin, TornadoService):
    """
    ..class:: FileCatalogHandler

    A simple Replica and Metadata Catalog service.
    """

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
            self.log.exception("Exception while sending seDump", repr(e))
            return S_ERROR(f"Exception while sendind seDump: {repr(e)}")
        finally:
            if csvOutput is not None:
                csvOutput.close()
