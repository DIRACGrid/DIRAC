"""
:mod: RssConfiguration

Module that collects utility functions.

"""
from DIRAC import S_OK
from DIRAC.Core.Utilities import List
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.ResourceStatusSystem.PolicySystem.StateMachine import RSSMachine
from DIRAC.ResourceStatusSystem.Utilities import Utils

_rssConfigPath = "ResourceStatus"


class RssConfiguration:
    """
    RssConfiguration::

      {
        Config:
        {
          State        : Active | InActive,
          Cache        : 300,
          FromAddress  : 'email@site.domain'
          StatusType   :
          {
            default       : all,
            StorageElement: ReadAccess, WriteAccess, CheckAccess, RemoveAccess
          }
        }
      }

    """

    def __init__(self):
        self.opsHelper = Operations()

    def getConfigCache(self, default=300):
        """
        Gets from <pathToRSSConfiguration>/Config the value of Cache
        """

        return self.opsHelper.getValue(f"{_rssConfigPath}/Config/Cache", default)

    def getConfigFromAddress(self, default=None):
        """
        Gets from <pathToRSSConfiguration>/Config the value of FromAddress
        """

        return self.opsHelper.getValue(f"{_rssConfigPath}/Config/FromAddress", default)

    def getConfigStatusType(self, elementType=None):
        """
        Gets all the status types per elementType, if not given, it takes default
        from CS. If not, hardcoded variable DEFAULT.
        """

        _DEFAULTS = ("all",)

        res = self.opsHelper.getOptionsDict(f"{_rssConfigPath}/Config/StatusTypes")

        if res["OK"]:
            if elementType in res["Value"]:
                return List.fromChar(res["Value"][elementType])

            if "default" in res["Value"]:
                return List.fromChar(res["Value"]["default"])

        return _DEFAULTS


def getPolicies():
    """
    Returns from the OperationsHelper: <_rssConfigPath>/Policies
    """

    return Utils.getCSTree(f"{_rssConfigPath}/Policies")


def getPolicyActions():
    """
    Returns from the OperationsHelper: <_rssConfigPath>/PolicyActions
    """

    return Utils.getCSTree(f"{_rssConfigPath}/PolicyActions")


def getnotificationGroups():
    """
    Returns from the OperationsHelper: <_rssConfigPath>/PolicyActions
    """

    return Utils.getCSTree(f"{_rssConfigPath}/Config")


def getNotifications():
    """
    Returns from the OperationsHelper: <_rssConfigPath>/Notification
    """

    return Utils.getCSTree(f"{_rssConfigPath}/Notification")


def getValidElements():
    """
    Returns from the OperationsHelper: <_rssConfigPath>/GeneralConfig/ValidElements
    """
    _DEFAULTS = ("Site", "Resource", "Node")

    #  result = Operations().getValue( '%s/GeneralConfig/ValidElements' % _rssConfigPath )
    #  if result is not None:
    #    return List.fromChar( result )
    return _DEFAULTS


def getValidStatus():
    """
    Returns a list of statuses as were defined on the RSS(State)Machine
    """

    validStatus = RSSMachine(None).getStates()
    return S_OK(validStatus)
