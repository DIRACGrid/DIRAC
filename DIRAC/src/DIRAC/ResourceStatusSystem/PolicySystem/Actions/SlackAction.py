""" SlackAction

  This action posts the status change to a Slack channel
  It uses Slack Webhooks.
  Also compatible with Mattermost, which is an open source alternative to Slack.
  To create a webhook URL refer the following :

  * Slack : https://api.slack.com/incoming-webhooks
  * Mattermost : https://docs.mattermost.com/developer/webhooks-incoming.html

  Add the webhook URL to dirac.cfg at Operations/[]/ResourceStatus/Config/Slack

  example:

    Operations/
      Defaults/
        ResourceStatus/
          Config/
            Slack = https://hooks.slack.com/services/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
            Mattermost = https://mattermost.web.cern.ch/hooks/axy94k3m1pg5xeyaw3qqb3x8bo

  Even if using Mattermost,the URL is still to be placed at
  Operations/[]/ResourceStatus/Config/Slack and not Operations/[]/ResourceStatus/Config/Mattermost

"""
import json
import requests

from DIRAC import S_ERROR, S_OK
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.ResourceStatusSystem.PolicySystem.Actions.BaseAction import BaseAction
from DIRAC.Core.Utilities.SiteSEMapping import getSitesForSE
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getCESiteMapping


class SlackAction(BaseAction):
    """
    Action that sends a brief Slack Message.
    """

    def __init__(self, name, decisionParams, enforcementResult, singlePolicyResults, clients=None, url=None):
        super().__init__(name, decisionParams, enforcementResult, singlePolicyResults, clients)
        if url is not None:
            self.url = url
        else:
            self.url = Operations().getValue("ResourceStatus/Config/Slack")

    def run(self):
        """
        Checks it has the parameters it needs and tries to send an sms to the users
        that apply.
        """

        if self.url is None:
            return S_ERROR("Slack URL not set")

        # Minor security checks
        element = self.decisionParams["element"]
        if element is None:
            return S_ERROR("element should not be None")

        name = self.decisionParams["name"]
        if name is None:
            return S_ERROR("name should not be None")

        statusType = self.decisionParams["statusType"]
        if statusType is None:
            return S_ERROR("statusType should not be None")

        previousStatus = self.decisionParams["status"]
        if previousStatus is None:
            return S_ERROR("status should not be None")

        status = self.enforcementResult["Status"]
        if status is None:
            return S_ERROR("status should not be None")

        reason = self.enforcementResult["Reason"]
        if reason is None:
            return S_ERROR("reason should not be None")

        if self.decisionParams["element"] == "Site":
            siteName = self.decisionParams["name"]
        else:
            elementType = self.decisionParams["elementType"]

            if elementType == "StorageElement":
                siteName = getSitesForSE(name)
            elif elementType == "ComputingElement":
                res = getCESiteMapping(name)
                if not res["OK"]:
                    self.log.error("Failure getting Site2CE mapping", res["Message"])
                    siteName = "ERROR"
                else:
                    siteName = res
            else:
                siteName = {"OK": True, "Value": "Unassigned"}

            if not siteName["OK"]:
                self.log.error(f"Resource {name} does not exist at any site: {siteName['Message']}")
                siteName = "Unassigned Resources"
            elif not siteName["Value"]:
                siteName = "Unassigned Resources"
            else:
                siteName = siteName["Value"] if isinstance(siteName["Value"], str) else siteName["Value"][0]

        message = f"*{name}* _{statusType}_ --> _{status}_ \n{reason}"
        return self.sendSlackMessage(message)

    def sendSlackMessage(self, message):
        """
        Sends a slack message to self.url

        :param str message: text message to send
        """

        payload = {"text": message}
        response = requests.post(self.url, data=json.dumps(payload), headers={"Content-Type": "application/json"})
        response.raise_for_status()
        return S_OK()
