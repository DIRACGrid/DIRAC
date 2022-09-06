"""Echo module based on the GFAL2_StorageBase class."""
import os
import random
import time
from timeit import default_timer

# from DIRAC
from DIRAC.Resources.Storage.GFAL2_StorageBase import GFAL2_StorageBase, setGfalSetting
from DIRAC import gLogger, S_ERROR, S_OK

# Duration in sec of a removal from which we start throttling
# The value is empirical
REMOVAL_DURATION_THROTTLE_LIMIT = 3

# Timeout for the removal operation
REMOVAL_TIMEOUT = 20


class EchoStorage(GFAL2_StorageBase):
    """.. class:: EchoStorage

    Interface to the Echo storage.

    This plugin will work with both gsiftp and root protocol.
    According to the RAL admins, Echo best use is done:

    * using gsiftp for all WAN transfers, and LAN write.
    * using root from LAN read.

    Note that there are still a few issues to be sorted out with xroot

    This would translate in a configuration such as::

      RAL-ECHO
      {
        BackendType = Echo
        SEType = T0D1
        AccessProtocols = gsiftp,root
        WriteProtocols = gsiftp
        XRootConfig
        {
          Host = xrootd.echo.stfc.ac.uk
          PluginName = Echo
          Protocol = root
          Path = lhcb:user
          Access = remote
        }
        GidFTPConfig
        {
          Host = gridftp.echo.stfc.ac.uk
          PluginName = Echo
          Protocol = gsiftp
          Path = lhcb:user
          Access = remote
        }
      }
      Operations
      {
        Defaults
        {
          DataManagement
          {
            ThirdPartyProtocols=srm,gsiftp,dips
            RegistrationProtocols=srm,gsiftp,dips
          }
        }
      }

    """

    def __init__(self, storageName, parameters):
        """c'tor"""
        # # init base class
        super().__init__(storageName, parameters)
        self.srmSpecificParse = False

        self.log = gLogger.getSubLogger(self.__class__.__name__)

        self.pluginName = "Echo"

        # Because Echo considers '<host>/lhcb:prod' differently from '<host>//lhcb:prod' as it normally should be
        # we need to disable the automatic normalization done by gfal2
        self.ctx.set_opt_boolean("XROOTD PLUGIN", "NORMALIZE_PATH", False)

        # This is in case the protocol is xroot
        # Because some storages are configured to use krb5 auth first
        # we end up in trouble for interactive sessions. This
        # environment variable enforces the use of certificates
        if self.protocolParameters["Protocol"] == "root" and "XrdSecPROTOCOL" not in os.environ:
            os.environ["XrdSecPROTOCOL"] = "gsi,unix"

        # We don't need extended attributes for metadata
        self._defaultExtendedAttributes = None

    def putDirectory(self, path):
        """Not available on Echo

        :returns: S_ERROR
        """
        return S_ERROR("Putting directory does not exist in Echo")

    def listDirectory(self, path):
        """Not available on Echo

        :returns: S_ERROR
        """
        return S_ERROR("Listing directory does not exist in Echo")

    def isDirectory(self, path):
        """Not available on Echo

        :returns: S_ERROR
        """
        return S_ERROR("Stating directory does not exist in Echo")

    def getDirectory(self, path, localPath=False):
        """Not available on Echo

        :returns: S_ERROR
        """
        return S_ERROR("Getting directory does not exist in Echo")

    def removeDirectory(self, path, recursive=False):
        """Not available on Echo

        :returns: S_ERROR
        """
        return S_ERROR("Removing directory does not exist in Echo")

    def getDirectorySize(self, path):
        """Not available on Echo

        :returns: S_ERROR
        """
        return S_ERROR("Getting directory size does not exist in Echo")

    def getDirectoryMetadata(self, path):
        """Not available on Echo

        :returns: S_ERROR
        """
        return S_ERROR("Getting directory metadata does not exist in Echo")

    def _createSingleDirectory(self, path):
        """Emulates creating directory on Echo by returning success (as Echo does)

        :returns: S_OK()
        """
        return S_OK()

    def _removeSingleFile(self, path):
        """Removal on Echo is unbearably slow.
        A ticket was opened, but the claim is that "it's CEPH, no can do"
        (https://ggus.eu/index.php?mode=ticket_info&ticket_id=140773)

        This throttles a bit the removal if we see we start taking too long.
        It is mostly useful in the context of the REA.
        """

        startTime = default_timer()
        with setGfalSetting(self.ctx, "CORE", "NAMESPACE_TIMEOUT", REMOVAL_TIMEOUT):
            # Because HTTP Plugin does not read the CORE:NAMESPACE_TIMEOUT as it should
            # I also specify it here
            with setGfalSetting(self.ctx, "HTTP PLUGIN", "OPERATION_TIMEOUT", REMOVAL_TIMEOUT):
                res = super()._removeSingleFile(path)
        duration = default_timer() - startTime

        # If it took too long, we sleep for a bit
        if duration > REMOVAL_DURATION_THROTTLE_LIMIT:
            time.sleep(random.uniform(0, 3))

        return res
