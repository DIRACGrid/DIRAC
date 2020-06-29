"""The FCOnly storage can only be used to register replicas in a FileCatalog.

It is used to preserve LFN metadata, no actual file content will be stored.

Example Configuration for Resources/StorageElement::

  ARCHIVE-SE
  {
    BackendType = None
    FCOnly
    {
      Access = remote
      Protocol = dfc
    }
  }

The 'dfc' protocol also needs to be added to the RegistrationProtocols and WriteProtocols lists in the
Operations/DataManagement section.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

from DIRAC import gLogger, S_OK
from DIRAC.Resources.Storage.Utilities import checkArgumentFormat
from DIRAC.Resources.Storage.StorageBase import StorageBase


class FCOnlyStorage(StorageBase):
  """MetaData Storage, only for registering LFNs, nothing else."""
  _INPUT_PROTOCOLS = ['dfc']
  _OUTPUT_PROTOCOLS = ['dfc']

  def __init__(self, storageName, parameters):
    """ c'tor

    :param self: self reference
    :param str storageName: SE name
    """
    StorageBase.__init__(self, storageName, parameters)
    self.log = gLogger.getSubLogger(__name__, True)

    self.pluginName = 'FCOnly'
    self.protocol = []

  def getTransportURL(self, pathDict, protocols):
    res = checkArgumentFormat(pathDict)
    if not res['OK']:
      return res
    urls = res['Value']
    successful = {}
    failed = {}

    for url in urls:
      successful[url] = url

    resDict = {'Failed': failed, 'Successful': successful}
    return S_OK(resDict)

  def getURLBase(self, withWSUrl=False):
    return S_OK(self.basePath)

  def removeFile(self, path):
    successful = {}
    for lfn in path:
      successful[lfn] = True
    resDict = {'Successful': successful, 'Failed': {}}
    return S_OK(resDict)
