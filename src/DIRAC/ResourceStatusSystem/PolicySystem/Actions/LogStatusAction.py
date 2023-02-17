""" LogStatusAction

"""
from DIRAC import S_ERROR
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
from DIRAC.ResourceStatusSystem.PolicySystem.Actions.BaseAction import BaseAction


class LogStatusAction(BaseAction):
    """
    Action that registers on the database a new entry on the <element>Status table.
    It adds or modifies if the record exists on the table.
    """

    def __init__(self, name, decisionParams, enforcementResult, singlePolicyResults, clients=None):
        super().__init__(name, decisionParams, enforcementResult, singlePolicyResults, clients)

        if clients is not None and "ResourceStatusClient" in clients:
            self.rsClient = clients["ResourceStatusClient"]
        else:
            self.rsClient = ResourceStatusClient()

    def run(self):
        """
        Checks it has the parameters it needs and tries to addOrModify in the
        database.
        """
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

        status = self.enforcementResult["Status"]
        if status is None:
            return S_ERROR("status should not be None")

        elementType = self.decisionParams["elementType"]
        if elementType is None:
            return S_ERROR("elementType should not be None")

        reason = self.enforcementResult["Reason"]
        if reason is None:
            return S_ERROR("reason should not be None")

        vo = self.enforcementResult.get("VO")
        # Truncate reason to fit in database column
        reason = (reason[:508] + "..") if len(reason) > 508 else reason
        # VO = 'all' (non-VO aware policy) for a combined policy affects all VOs for a given site or resource,
        if vo == "all":
            resSelect = self.rsClient.selectStatusElement(
                element,
                "Status",
                name=name,
                statusType=None,
                vO=None,
                status=None,
                elementType=None,
                reason=None,
                dateEffective=None,
                lastCheckTime=None,
                tokenOwner="rs_svc",
                tokenExpiration=None,
                meta=None,
            )
            if not resSelect["OK"]:
                self.log.error(f"Could not obtain all VO rows for element: {element}")
                return resSelect
            voColumnIndex = resSelect["Columns"].index("VO")
            for row in resSelect["Value"]:
                vo = row[voColumnIndex]
                resLogUpdate = self.rsClient.addOrModifyStatusElement(
                    element,
                    "Status",
                    name=name,
                    statusType=statusType,
                    vO=vo,
                    status=status,
                    elementType=elementType,
                    reason=reason,
                )
                self.log.debug("Update result", resLogUpdate)

        else:
            resLogUpdate = self.rsClient.addOrModifyStatusElement(
                element,
                "Status",
                name=name,
                statusType=statusType,
                vO=vo,
                status=status,
                elementType=elementType,
                reason=reason,
            )

        return resLogUpdate
