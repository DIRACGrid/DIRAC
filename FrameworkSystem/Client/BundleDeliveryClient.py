""" Client for interacting with Framework/BundleDelivery service
"""

import os
import io
import tarfile
import cStringIO

from DIRAC import S_OK, gLogger
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
    if self.transferClient:
      return self.transferClient
    return TransferClient("Framework/BundleDelivery",
                          skipCACheck=skipCACheck())

  def __getHash(self, bundleID, dirToSyncTo):
    try:
      with io.open(os.path.join(dirToSyncTo, ".dab.%s" % bundleID), "rb") as fd:
        bdHash = fd.read().strip()
        return bdHash
    except BaseException:
      return ""

  def __setHash(self, bundleID, dirToSyncTo, bdHash):
    try:
      fileName = os.path.join(dirToSyncTo, ".dab.%s" % bundleID)
      with io.open(fileName, "wb") as fd:
        fd.write(bdHash)
    except Exception as e:
      self.log.error("Could not save hash after synchronization", "%s: %s" % (fileName, str(e)))

  def syncDir(self, bundleID, dirToSyncTo):
    dirCreated = False
    if not os.path.isdir(dirToSyncTo):
      self.log.info("Creating dir %s" % dirToSyncTo)
      mkDir(dirToSyncTo)
      dirCreated = True
    currentHash = self.__getHash(bundleID, dirToSyncTo)
    self.log.info("Current hash for bundle %s in dir %s is '%s'" % (bundleID, dirToSyncTo, currentHash))
    buff = cStringIO.StringIO()
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
        tF.extract(tarinfo, dirToSyncTo)
    buff.close()
    self.__setHash(bundleID, dirToSyncTo, newHash)
    self.log.info("Dir has been synchronized")
    return S_OK(True)

  def syncCAs(self):
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
    X509_CERT_DIR = False
    if 'X509_CERT_DIR' in os.environ:
      X509_CERT_DIR = os.environ['X509_CERT_DIR']
      del os.environ['X509_CERT_DIR']
    result = self.syncDir("CRLs", Locations.getCAsLocation())
    if X509_CERT_DIR:
      os.environ['X509_CERT_DIR'] = X509_CERT_DIR
    return result

  def getCAs(self):
    """
    This method can be used to create the CAs. If the file can not be created, it will be downloaded from
    the server.
    """
    retVal = Utilities.generateCAFile()
    if not retVal['OK']:
      # if we can not found the file, we return the directory, where the file should be
      transferClient = self.__getTransferClient()
      casFile = os.path.join(os.path.dirname(retVal['Message']), "cas.pem")
      with io.open(casFile, "w") as fd:
        result = transferClient.receiveFile(fd, 'CAs')
        if not result['OK']:
          return result
        return S_OK(casFile)
    else:
      return retVal

  def getCLRs(self):
    """
    This method can be used to create the CAs. If the file can not be created, it will be downloaded from
    the server.
    """
    retVal = Utilities.generateRevokedCertsFile()
    if not retVal['OK']:
      # if we can not found the file, we return the directory, where the file should be
      transferClient = self.__getTransferClient()
      casFile = os.path.join(os.path.dirname(retVal['Message']), "crls.pem")
      with io.open(casFile, "w") as fd:
        result = transferClient.receiveFile(fd, 'CRLs')
        if not result['OK']:
          return result
        return S_OK(casFile)
    else:
      return retVal
