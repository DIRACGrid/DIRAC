"""
:mod: RssConfiguration

Module that collects utility functions.

"""

from DIRAC import S_OK
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
          Cache        : 300,
          FromAddress  : 'email@site.domain'
          Policies
          {
          }
          PolicyActions
          {
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
