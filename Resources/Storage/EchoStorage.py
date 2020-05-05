"""Echo module based on the GFAL2_StorageBase class."""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


import os

# from DIRAC
from DIRAC.Resources.Storage.GFAL2_StorageBase import GFAL2_StorageBase
from DIRAC import gLogger, S_ERROR, S_OK


class EchoStorage(GFAL2_StorageBase):
  """ .. class:: EchoStorage

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
    """ c'tor
    """
    # # init base class
    super(EchoStorage, self).__init__(storageName, parameters)
    self.srmSpecificParse = False

    self.log = gLogger.getSubLogger("EchoStorage")

    self.pluginName = 'Echo'

    # Because Echo considers '<host>/lhcb:prod' differently from '<host>//lhcb:prod' as it normally should be
    # we need to disable the automatic normalization done by gfal2
    self.ctx.set_opt_boolean("XROOTD PLUGIN", "NORMALIZE_PATH", False)

    # This is in case the protocol is xroot
    # Because some storages are configured to use krb5 auth first
    # we end up in trouble for interactive sessions. This
    # environment variable enforces the use of certificates
    if self.protocolParameters['Protocol'] == 'root' and 'XrdSecPROTOCOL' not in os.environ:
      os.environ['XrdSecPROTOCOL'] = 'gsi,unix'

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
    """ Emulates creating directory on Echo by returning success (as Echo does)

        :returns: S_OK()
    """
    return S_OK()
