""" The CS! (Configuration Service)

  Modified to work with Tornado
  Encode data in base64 because of JSON limitations
  In client side you must use a specific client

"""

__RCSID__ = "$Id$"

from base64 import b64encode, b64decode

from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR
from DIRAC.ConfigurationSystem.private.ServiceInterfaceTornado import ServiceInterfaceTornado as ServiceInterface
from DIRAC.Core.Utilities import DErrno
from DIRAC.TornadoServices.Server.TornadoService import TornadoService




class TornadoConfigurationHandler(TornadoService):
  """ The CS handler
  """
  ServiceInterface = None
  PilotSynchronizer = None

  @classmethod
  def initializeHandler(cls, serviceInfo):
    """
      Initialize the configuration server
      Behind it start thread who refresh configuration
    """
    cls.ServiceInterface = ServiceInterface(serviceInfo['URL'])
    return S_OK()


  def export_getVersion(self):
    return S_OK(self.ServiceInterface.getVersion())


  def export_getCompressedData(self):
    sData = self.ServiceInterface.getCompressedConfigurationData()
    return S_OK(b64encode(sData))


  def export_getCompressedDataIfNewer(self, sClientVersion):
    sVersion = self.ServiceInterface.getVersion()
    retDict = {'newestVersion': sVersion}
    if sClientVersion < sVersion:
      retDict['data'] = b64encode(self.ServiceInterface.getCompressedConfigurationData())
    return S_OK(retDict)


  def export_publishSlaveServer(self, sURL):
    self.ServiceInterface.publishSlaveServer(sURL)
    return S_OK()


  def export_commitNewData(self, sData):
    credDict = self.getRemoteCredentials()
    if 'DN' not in credDict or 'username' not in credDict:
      return S_ERROR("You must be authenticated!")
    sData = b64decode(sData)
    res = self.ServiceInterface.updateConfiguration(sData, credDict['username'])
    if not res['OK']:
      return res

    # Check the flag for updating the pilot 3 JSON file
    if self.srv_getCSOption('UpdatePilotCStoJSONFile', False) and self.ServiceInterface.isMaster():
      if self.PilotSynchronizer is None:
        try:
          # This import is only needed for the Master CS service, making it conditional avoids
          # dependency on the git client preinstalled on all the servers running CS slaves
          from DIRAC.WorkloadManagementSystem.Utilities.PilotCStoJSONSynchronizer import PilotCStoJSONSynchronizer
        except ImportError as exc:
          self.log.exception("Failed to import PilotCStoJSONSynchronizer", repr(exc))
          return S_ERROR(DErrno.EIMPERR, 'Failed to import PilotCStoJSONSynchronizer')
        self.PilotSynchronizer = PilotCStoJSONSynchronizer()
      return self.PilotSynchronizer.sync()

    return res


  def export_writeEnabled(self):
    return S_OK(self.ServiceInterface.isMaster())


  def export_getCommitHistory(self, limit=100):
    if limit > 100:
      limit = 100
    history = self.ServiceInterface.getCommitHistory()
    if limit:
      history = history[:limit]
    return S_OK(history)


  def export_getVersionContents(self, versionList):
    contentsList = []
    for version in versionList:
      retVal = self.ServiceInterface.getVersionContents(version)
      if retVal['OK']:
        contentsList.append(retVal['Value'])
      else:
        return S_ERROR("Can't get contents for version %s: %s" % (version, retVal['Message']))
    return S_OK(contentsList)


  def export_rollbackToVersion(self, version):
    retVal = self.ServiceInterface.getVersionContents(version)
    if not retVal['OK']:
      return S_ERROR("Can't get contents for version %s: %s" % (version, retVal['Message']))
    credDict = self.getRemoteCredentials()
    if 'DN' not in credDict or 'username' not in credDict:
      return S_ERROR("You must be authenticated!")
    return self.ServiceInterface.updateConfiguration(retVal['Value'],
                                                 credDict['username'],
                                                 updateVersionOption=True)
