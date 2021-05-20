""" Handler for CAs + CRLs bundles
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import six
import tarfile
import os
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import gLogger, S_OK, S_ERROR, gConfig
from DIRAC.Core.Utilities.ThreadScheduler import gThreadScheduler
from DIRAC.Core.Utilities import File, List
from DIRAC.Core.Security import Locations, Utilities


class BundleManager(object):

  def __init__(self, baseCSPath):
    self.__csPath = baseCSPath
    self.__bundles = {}
    self.updateBundles()

  def __getDirsToBundle(self):
    dirsToBundle = {}
    result = gConfig.getOptionsDict("%s/DirsToBundle" % self.__csPath)
    if result['OK']:
      dB = result['Value']
      for bId in dB:
        dirsToBundle[bId] = List.fromChar(dB[bId])
    if gConfig.getValue("%s/BundleCAs" % self.__csPath, True):
      dirsToBundle['CAs'] = [
          "%s/*.0" %
          Locations.getCAsLocation(),
          "%s/*.signing_policy" %
          Locations.getCAsLocation(),
          "%s/*.pem" %
          Locations.getCAsLocation()]
    if gConfig.getValue("%s/BundleCRLs" % self.__csPath, True):
      dirsToBundle['CRLs'] = ["%s/*.r0" % Locations.getCAsLocation()]
    return dirsToBundle

  def getBundles(self):
    return dict([(bId, self.__bundles[bId]) for bId in self.__bundles])

  def bundleExists(self, bId):
    return bId in self.__bundles

  def getBundleVersion(self, bId):
    try:
      return self.__bundles[bId][0]
    except Exception:
      return ""

  def getBundleData(self, bId):
    try:
      return self.__bundles[bId][1]
    except Exception:
      return ""

  def updateBundles(self):
    dirsToBundle = self.__getDirsToBundle()
    # Delete bundles that don't have to be updated
    for bId in self.__bundles:
      if bId not in dirsToBundle:
        gLogger.info("Deleting old bundle %s" % bId)
        del(self.__bundles[bId])
    for bId in dirsToBundle:
      bundlePaths = dirsToBundle[bId]
      gLogger.info("Updating %s bundle %s" % (bId, bundlePaths))
      buffer_ = six.BytesIO()
      filesToBundle = sorted(File.getGlobbedFiles(bundlePaths))
      if filesToBundle:
        commonPath = os.path.commonprefix(filesToBundle)
        commonEnd = len(commonPath)
        gLogger.info("Bundle will have %s files with common path %s" % (len(filesToBundle), commonPath))
        with tarfile.open('dummy', "w:gz", buffer_) as tarBuffer:
          for filePath in filesToBundle:
            tarBuffer.add(filePath, filePath[commonEnd:])
        zippedData = buffer_.getvalue()
        buffer_.close()
        hash_ = File.getMD5ForFiles(filesToBundle)
        gLogger.info("Bundled %s : %s bytes (%s)" % (bId, len(zippedData), hash_))
        self.__bundles[bId] = (hash_, zippedData)
      else:
        self.__bundles[bId] = (None, None)


class BundleDeliveryHandler(RequestHandler):

  @classmethod
  def initializeHandler(cls, serviceInfoDict):
    csPath = serviceInfoDict['serviceSectionPath']
    cls.bundleManager = BundleManager(csPath)
    updateBundleTime = gConfig.getValue("%s/BundlesLifeTime" % csPath, 3600 * 6)
    gLogger.info("Bundles will be updated each %s secs" % updateBundleTime)
    gThreadScheduler.addPeriodicTask(updateBundleTime, cls.bundleManager.updateBundles)
    return S_OK()

  types_getListOfBundles = []

  @classmethod
  def export_getListOfBundles(cls):
    return S_OK(cls.bundleManager.getBundles())

  def transfer_toClient(self, fileId, token, fileHelper):
    version = ""
    if isinstance(fileId, six.string_types):
      if fileId in ['CAs', 'CRLs']:
        return self.__transferFile(fileId, fileHelper)
      else:
        bId = fileId
    elif isinstance(fileId, (list, tuple)):
      if len(fileId) == 0:
        fileHelper.markAsTransferred()
        return S_ERROR("No bundle specified!")
      elif len(fileId) == 1:
        bId = fileId[0]
      else:
        bId = fileId[0]
        version = fileId[1]
    if not self.bundleManager.bundleExists(bId):
      fileHelper.markAsTransferred()
      return S_ERROR("Unknown bundle %s" % bId)

    bundleVersion = self.bundleManager.getBundleVersion(bId)
    if bundleVersion is None:
      fileHelper.markAsTransferred()
      return S_ERROR("Empty bundle %s" % bId)

    if version == bundleVersion:
      fileHelper.markAsTransferred()
      return S_OK(bundleVersion)

    buffer_ = six.BytesIO(self.bundleManager.getBundleData(bId))
    result = fileHelper.DataSourceToNetwork(buffer_)
    buffer_.close()
    if not result['OK']:
      return result
    return S_OK(bundleVersion)

  def __transferFile(self, filetype, fileHelper):
    """
    This file is creates and transfers the CAs or CRLs file to the client.
    :param str filetype: we can define which file will be transfered to the client
    :param object fileHelper:
    :return: S_OK or S_ERROR
    """
    if filetype == 'CAs':
      retVal = Utilities.generateCAFile()
    elif filetype == 'CRLs':
      retVal = Utilities.generateRevokedCertsFile()
    else:
      return S_ERROR("Not supported file type %s" % filetype)

    if not retVal['OK']:
      return retVal
    else:
      result = fileHelper.getFileDescriptor(retVal['Value'], 'r')
      if not result['OK']:
        result = fileHelper.sendEOF()
        # better to check again the existence of the file
        if not os.path.exists(retVal['Value']):
          return S_ERROR('File %s does not exist' % os.path.basename(retVal['Value']))
        else:
          return S_ERROR('Failed to get file descriptor')
      fileDescriptor = result['Value']
      result = fileHelper.FDToNetwork(fileDescriptor)
      fileHelper.oFile.close()  # close the file and return
      return result
