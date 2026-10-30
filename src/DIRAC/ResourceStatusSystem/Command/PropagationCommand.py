"""
PropagationCommand module
This command gets all the elements that exist under a given site and applies the following logic:
if even one element is 'Active' for the given site then it marks the site as 'Active', if all elements
are set in a different status (like banned or error) then it marks the site as 'Banned'
"""
from DIRAC import S_OK, S_ERROR
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
from DIRAC.ResourceStatusSystem.Command.Command import Command
from DIRAC.ResourceStatusSystem.Utilities import CSHelpers


class PropagationCommand(Command):
    def __init__(self, args=None, clients=None):
        self.rssClient = ResourceStatusClient()
        super().__init__(args, clients)

    def doNew(self, masterParams=None):
        return S_OK()

    def doCache(self):
        if not self.args.get("name"):
            return S_OK({"Status": "Unknown", "Reason": "No name provided for propagation check"})

        site = self.args["name"]

        elements = CSHelpers.getSiteElements(site)
        if not elements["OK"]:
            return S_OK({"Status": "Unknown", "Reason": f"Could not get site elements: {elements['Message']}"})

        statusList = []

        for element in elements["Value"]:
            status = self.rssClient.selectStatusElement("Resource", "Status", element, meta={"columns": ["Status"]})
            if not status["OK"]:
                self.log.warn(f"Could not get status for {element}: {status['Message']}")
                continue

            if status["Value"]:
                statusList.append(status["Value"][0][0])
            else:  # forcing in the case the resource has no status (yet)
                statusList.append("Active")

        if not statusList:
            return S_OK({"Status": "Unknown", "Reason": "No elements found or all queries failed"})

        if "Active" in statusList:
            return S_OK({"Status": "Active", "Reason": "An element that belongs to the site is Active"})

        if "Degraded" in statusList:
            return S_OK({"Status": "Degraded", "Reason": "An element that belongs to the site is Degraded"})

        return S_OK({"Status": "Banned", "Reason": "There is no Active element in the site"})

    def doMaster(self):
        return S_OK()
