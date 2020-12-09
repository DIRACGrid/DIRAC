""" Client for interacting with Framework/BundleDelivery service
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import getpass
import tarfile
from six import BytesIO

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Base.Client import Client, createClient
from DIRAC.Core.DISET.TransferClient import TransferClient
from DIRAC.Core.Security import Locations, Utilities
from DIRAC.Core.Utilities.File import mkDir
from DIRAC.ConfigurationSystem.Client.Helpers.CSGlobals import skipCACheck


__RCSID__ = "$Id$"


@createClient('Framework/BundleDelivery')
class BundleDeliveryClient(Client):

  def __init__(self, transferClient=False, **kwargs):
    super(BundleDeliveryClient, self).__init__(**kwargs)
    self.setServer('Framework/BundleDelivery')
    self.transferClient = transferClient
    self.log = gLogger.getSubLogger("BundleDelivery")

  def __getTransferClient(self):
    """ Get transfer client

        :return: TransferClient()
    """
    if self.transferClient:
      return self.transferClient
    return TransferClient("Framework/BundleDelivery",
                          skipCACheck=skipCACheck())

  def __getHash(self, bundleID, dirToSyncTo):
    """ Get hash for bundle in directory

        :param str bundleID: bundle ID
        :param str dirToSyncTo: path to sync directory

        :return: str
    """
    try:
      with open(os.path.join(dirToSyncTo, ".dab.%s" % bundleID), "r") as fd:
        bdHash = fd.read().strip()
        return bdHash
    except BaseException:
      return ""

  def __setHash(self, bundleID, dirToSyncTo, bdHash):
    """ Set hash for bundle in directory

        :param str bundleID: bundle ID
        :param str dirToSyncTo: path to sync directory
        :param str bdHash: new hash
    """
    try:
      fileName = os.path.join(dirToSyncTo, ".dab.%s" % bundleID)
      with open(fileName, "wt") as fd:
        fd.write(bdHash)
    except Exception as e:
      self.log.error("Could not save hash after synchronization", "%s: %s" % (fileName, str(e)))

  def syncDir(self, bundleID, dirToSyncTo):
    """ Synchronize directory

        :param str bundleID: bundle ID
        :param str dirToSyncTo: path to sync directory

        :return: S_OK(bool)/S_ERROR()
    """
    dirCreated = False
    if os.path.isdir(dirToSyncTo):
      for p in [os.W_OK, os.R_OK]:
        if not os.access(dirToSyncTo, p):
          self.log.error('%s does not have the permissions to update %s' % (getpass.getuser(), dirToSyncTo))
          return S_ERROR('%s does not have the permissions to update %s' % (getpass.getuser(), dirToSyncTo))
    else:
      self.log.info("Creating dir %s" % dirToSyncTo)
      mkDir(dirToSyncTo)
      dirCreated = True
    currentHash = self.__getHash(bundleID, dirToSyncTo)
    self.log.info("Current hash for bundle %s in dir %s is '%s'" % (bundleID, dirToSyncTo, currentHash))
    buff = BytesIO()
    transferClient = self.__getTransferClient()
    result = transferClient.receiveFile(buff, [bundleID, currentHash])
    if not result['OK']:
      self.log.error("Could not sync dir", result['Message'])
      if dirCreated:
        self.log.info("Removing dir %s" % dirToSyncTo)
        os.unlink(dirToSyncTo)
      buff.close()
      return result
    newHash = result['Value']
    if newHash == currentHash:
      self.log.info("Dir %s was already in sync" % dirToSyncTo)
      return S_OK(False)
    buff.seek(0)
    self.log.info("Synchronizing dir with remote bundle")
    with tarfile.open(name='dummy', mode="r:gz", fileobj=buff) as tF:
      for tarinfo in tF:
        try:
          tF.extract(tarinfo, dirToSyncTo)
        except OSError as e:
          self.log.error("Could not sync dir:", str(e))
          if dirCreated:
            self.log.info("Removing dir %s" % dirToSyncTo)
            os.unlink(dirToSyncTo)
          buff.close()
          return S_ERROR("Certificates directory update failed: %s" % str(e))

    buff.close()
    self.__setHash(bundleID, dirToSyncTo, newHash)
    self.log.info("Dir has been synchronized")
    return S_OK(True)

  def syncCAs(self):
    """ Synchronize CAs

        :return: S_OK(bool)/S_ERROR()
    """
    X509_CERT_DIR = False
    if 'X509_CERT_DIR' in os.environ:
      X509_CERT_DIR = os.environ['X509_CERT_DIR']
      del os.environ['X509_CERT_DIR']
    casLocation = Locations.getCAsLocation()
    if not casLocation:
      casLocation = Locations.getCAsDefaultLocation()
    result = self.syncDir("CAs", casLocation)
    if X509_CERT_DIR:
      os.environ['X509_CERT_DIR'] = X509_CERT_DIR
    return result

  def syncCRLs(self):
    """ Synchronize CRLs

        :return: S_OK(bool)/S_ERROR()
    """
    X509_CERT_DIR = False
    if 'X509_CERT_DIR' in os.environ:
      X509_CERT_DIR = os.environ['X509_CERT_DIR']
      del os.environ['X509_CERT_DIR']
    result = self.syncDir("CRLs", Locations.getCAsLocation())
    if X509_CERT_DIR:
      os.environ['X509_CERT_DIR'] = X509_CERT_DIR
    return result

  def getCAs(self):
    """ This method can be used to create the CAs. If the file can not be created,
        it will be downloaded from the server.

        :return: S_OK(str)/S_ERROR()
    """
    retVal = Utilities.generateCAFile()
    if not retVal['OK']:
      self.log.warn("Could not generate/find CA file", retVal['Message'])
      # if we can not found the file, we return the directory, where the file should be
      transferClient = self.__getTransferClient()
      casFile = os.path.join(os.path.dirname(retVal['Message']), "cas.pem")
      with open(casFile, "w") as fd:
        result = transferClient.receiveFile(fd, 'CAs')
        if not result['OK']:
          return result
        return S_OK(casFile)
    else:
      return retVal

  def getCLRs(self):
    """ This method can be used to create the CRLs. If the file can not be created,
        it will be downloaded from the server.

        :return: S_OK(str)/S_ERROR()
    """
    retVal = Utilities.generateRevokedCertsFile()
    if not retVal['OK']:
      # if we can not found the file, we return the directory, where the file should be
      transferClient = self.__getTransferClient()
      casFile = os.path.join(os.path.dirname(retVal['Message']), "crls.pem")
      with open(casFile, "w") as fd:
        result = transferClient.receiveFile(fd, 'CRLs')
        if not result['OK']:
          return result
        return S_OK(casFile)
    else:
      return retVal
