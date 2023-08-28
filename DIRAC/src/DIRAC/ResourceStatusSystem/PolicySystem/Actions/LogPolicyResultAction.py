""" LogPolicyResultAction

"""
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.ResourceStatusSystem.PolicySystem.Actions.BaseAction import BaseAction


class LogPolicyResultAction(BaseAction):
    """
    Action that registers on the database a new entry per policy result in the
    list singlePolicyResults.
    """

    def __init__(self, name, decisionParams, enforcementResult, singlePolicyResults, clients=None):
        super().__init__(name, decisionParams, enforcementResult, singlePolicyResults, clients)

        if clients is not None and "ResourceManagementClient" in clients:
            self.rmClient = clients["ResourceManagementClient"]
        else:
            result = ObjectLoader().loadObject("DIRAC.ResourceStatusSystem.Client.ResourceManagementClient")
            if not result["OK"]:
                raise Exception(result["Message"])
            resourceManagementClientClass = result["Value"]
            self.rmClient = resourceManagementClientClass()

    def run(self):
        """
        Checks it has the parameters it needs and tries to addOrModify in the
        database.
        """

        element = self.decisionParams["element"]
        if element is None:
            return S_ERROR("element should not be None")

        name = self.decisionParams["name"]
        if name is None:
            return S_ERROR("name should not be None")

        statusType = self.decisionParams["statusType"]
        if statusType is None:
            return S_ERROR("statusType should not be None")

        for singlePolicyResult in self.singlePolicyResults:
            status = singlePolicyResult["Status"]
            if status is None:
                return S_ERROR("status should not be None")

            reason = singlePolicyResult["Reason"]
            if reason is None:
                return S_ERROR("reason should not be None")

            policyName = singlePolicyResult["Policy"]["name"]
            if policyName is None:
                return S_ERROR("policyName should not be None")

            vo = singlePolicyResult.get("VO")
            # Truncate reason to fit in database column
            reason = (reason[:508] + "..") if len(reason) > 508 else reason

            polUpdateRes = self.rmClient.addOrModifyPolicyResult(
                element=element,
                name=name,
                vO=vo,
                policyName=policyName,
                statusType=statusType,
                status=status,
                reason=reason,
            )

            if not polUpdateRes["OK"]:
                return polUpdateRes

        return S_OK()
