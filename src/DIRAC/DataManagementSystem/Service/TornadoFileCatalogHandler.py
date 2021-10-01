########################################################################
# File: FileCatalogHandler.py
########################################################################
"""
:mod: FileCatalogHandler

.. module: FileCatalogHandler

:synopsis: FileCatalogHandler is a simple Replica and Metadata Catalog service

"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

# imports
import csv

from six import StringIO

# from DIRAC

from DIRAC import gLogger, S_ERROR
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

    def export_streamToClient(self, seName):
        """This method used to transfer the SEDump to the client,
        formated as CSV with '|' separation

        :param seName: name of the se to dump

        :returns: the result of the FileHelper


        """

        retVal = self.getSEDump(seName)

        try:
            csvOutput = StringIO()
            writer = csv.writer(csvOutput, delimiter="|")
            writer.writerows(retVal)

            ret = csvOutput.getvalue()
            return ret

        except Exception as e:
            sLog.exception("Exception while sending seDump", repr(e))
            return S_ERROR("Exception while sendind seDump: %s" % repr(e))
        finally:
            csvOutput.close()
