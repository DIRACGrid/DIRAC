"""GSIFTP module based on the GFAL2_StorageBase class."""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

# from DIRAC
from DIRAC.Resources.Storage.GFAL2_StorageBase import GFAL2_StorageBase
from DIRAC import gLogger
from DIRAC.Core.Utilities.Pfn import pfnparse, pfnunparse


class GFAL2_GSIFTPStorage(GFAL2_StorageBase):

  """ .. class:: GFAL2_GSIFTPStorage

  GSIFTP interface to StorageElement using gfal2
  """
  _INPUT_PROTOCOLS = ['file', 'gsiftp']
  _OUTPUT_PROTOCOLS = ['gsiftp']

  def __init__(self, storageName, parameters):
    """ c'tor
    """
    # # init base class
    super(GFAL2_GSIFTPStorage, self).__init__(storageName, parameters)
    self.srmSpecificParse = False

    self.log = gLogger.getSubLogger("GFAL2_GSIFTPStorage")

    self.pluginName = 'GFAL2_GSIFTP'

    # We don't need extended attributes for metadata
    self._defaultExtendedAttributes = None

  def __addDoubleSlash(self, res):
    """ Utilities to add the double slash between the host(:port) and the path

        :param res: DIRAC return structure which contains an URL if S_OK
        :return: DIRAC structure with corrected URL
    """
    if not res['OK']:
      return res
    url = res['Value']
    res = pfnparse(url, srmSpecific=self.srmSpecificParse)
    if not res['OK']:
      return res
    urlDict = res['Value']
    urlDict['Path'] = '/' + urlDict['Path']
    return pfnunparse(urlDict, srmSpecific=self.srmSpecificParse)

  def getURLBase(self, withWSUrl=False):
    """ Overwrite to add the double slash """
    return self.__addDoubleSlash(super(GFAL2_GSIFTPStorage, self).getURLBase(withWSUrl=withWSUrl))

  def constructURLFromLFN(self, lfn, withWSUrl=False):
    """ Overwrite to add the double slash """
    return self.__addDoubleSlash(super(GFAL2_GSIFTPStorage, self).constructURLFromLFN(lfn=lfn, withWSUrl=withWSUrl))

  def getCurrentURL(self, fileName):
    """ Overwrite to add the double slash """
    return self.__addDoubleSlash(super(GFAL2_GSIFTPStorage, self).getCurrentURL(fileName))
