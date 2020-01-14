""" The CS! (Configuration Service)
"""

__RCSID__ = "$Id$"

from DIRAC.ConfigurationSystem.private.ServiceInterface import ServiceInterface
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities import DErrno
from DIRAC.Core.Security.Properties import CS_ADMINISTRATOR

gServiceInterface = None
gPilotSynchronizer = None


def initializeConfigurationHandler(serviceInfo):
  global gServiceInterface
  gServiceInterface = ServiceInterface(serviceInfo['URL'])
  return S_OK()


class ConfigurationHandler(RequestHandler):
  """ The CS handler
  """

  types_getVersion = []

  @classmethod
  def export_getVersion(cls):
    return S_OK(gServiceInterface.getVersion())

  types_getCompressedData = []

  @classmethod
  def export_getCompressedData(cls):
    sData = gServiceInterface.getCompressedConfigurationData()
    return S_OK(sData)

  types_getCompressedDataIfNewer = [basestring]

  @classmethod
  def export_getCompressedDataIfNewer(cls, sClientVersion):
    sVersion = gServiceInterface.getVersion()
    retDict = {'newestVersion': sVersion}
    if sClientVersion < sVersion:
      retDict['data'] = gServiceInterface.getCompressedConfigurationData()
    return S_OK(retDict)

  types_publishSlaveServer = [basestring]

  @classmethod
  def export_publishSlaveServer(cls, sURL):
    gServiceInterface.publishSlaveServer(sURL)
    return S_OK()

  types_commitNewData = [basestring]

  def export_commitNewData(self, sData):
    global gPilotSynchronizer
    credDict = self.getRemoteCredentials()
    if not credDict.get('username') or credDict['username'] == 'anonymous':
      return S_ERROR("You must be authenticated!")
    res = gServiceInterface.updateConfiguration(sData, credDict['username'])
    if not res['OK']:
      return res

    # Check the flag for updating the pilot 3 JSON file
    updatePilotCStoJSONFileFlag = Operations().getValue('Pilot/UpdatePilotCStoJSONFile', True)
    if updatePilotCStoJSONFileFlag in ['no', 'No', False, 'False', 'false']:
      updatePilotCStoJSONFileFlag = False

    if updatePilotCStoJSONFileFlag and gServiceInterface.isMaster():
      if gPilotSynchronizer is None:
        try:
          # This import is only needed for the Master CS service, making it conditional avoids
          # dependency on the git client pre-installed on all the servers running CS slaves
          from DIRAC.WorkloadManagementSystem.Utilities.PilotCStoJSONSynchronizer import PilotCStoJSONSynchronizer
        except ImportError as exc:
          self.log.exception("Failed to import PilotCStoJSONSynchronizer", repr(exc))
          return S_ERROR(DErrno.EIMPERR, 'Failed to import PilotCStoJSONSynchronizer')
        gPilotSynchronizer = PilotCStoJSONSynchronizer()
      return gPilotSynchronizer.sync()

    return res

  types_forceGlobalConfigurationUpdate = []
  auth_forceGlobalConfigurationUpdate = [CS_ADMINISTRATOR]

  def export_forceGlobalConfigurationUpdate(self):
    """
    Attempt to request all the configured services to update their configuration

    :return: S_OK, Value Successful/Failed dict with service URLs
    """
    return gServiceInterface.forceGlobalUpdate()

  types_writeEnabled = []

  @classmethod
  def export_writeEnabled(cls):
    return S_OK(gServiceInterface.isMaster())

  types_getCommitHistory = []

  @classmethod
  def export_getCommitHistory(cls, limit=100):
    if limit > 100:
      limit = 100
    history = gServiceInterface.getCommitHistory()
    if limit:
      history = history[:limit]
    return S_OK(history)

  types_getVersionContents = [list]

  @classmethod
  def export_getVersionContents(cls, versionList):
    contentsList = []
    for version in versionList:
      retVal = gServiceInterface.getVersionContents(version)
      if retVal['OK']:
        contentsList.append(retVal['Value'])
      else:
        return S_ERROR("Can't get contents for version %s: %s" % (version, retVal['Message']))
    return S_OK(contentsList)

  types_rollbackToVersion = [basestring]

  def export_rollbackToVersion(self, version):
    retVal = gServiceInterface.getVersionContents(version)
    if not retVal['OK']:
      return S_ERROR("Can't get contents for version %s: %s" % (version, retVal['Message']))
    credDict = self.getRemoteCredentials()
    if not credDict.get('username') or credDict['username'] == 'anonymous':
      return S_ERROR("You must be authenticated!")
    return gServiceInterface.updateConfiguration(retVal['Value'],
                                                 credDict['username'],
                                                 updateVersionOption=True)
