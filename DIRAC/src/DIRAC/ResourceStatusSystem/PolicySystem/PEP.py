""" PEP

  PEP ( Policy Enforcement Point ) is the front-end of the whole Policy System.
  Any interaction with it must go through the PEP to ensure a smooth flow.

  Firstly, it loads the PDP ( Policy Decision Point ) which actually is the
  module doing all dirty work ( finding policies, running them, merging their
  results, etc... ). Indeed, the PEP takes the output of the PDP for a given set
  of parameters ( decisionParams ) and enforces the actions that apply ( also
  determined by the PDP output ).

"""
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.ResourceStatusSystem.PolicySystem.PDP import PDP
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader


class PEP:
    """PEP ( Policy Enforcement Point )"""

    def __init__(self, clients=dict()):
        """Constructor

        examples:
          >>> pep = PEP()
          >>> pep1 = PEP( { 'ResourceStatusClient' : ResourceStatusClient() } )
          >>> pep2 = PEP( { 'ResourceStatusClient' : ResourceStatusClient(), 'ClientY' : None } )

        :Parameters:
          **clients** - [ None, `dict` ]
            dictionary with clients to be used in the commands issued by the policies.
            If not defined, the commands will import them. It is a measure to avoid
            opening the same connection every time a policy is evaluated.

        """

        self.clients = dict(clients)

        self.objectLoader = ObjectLoader()

        # Creating the client in the PEP is a convenience for the PDP, that uses internally the RSS clients

        res = self.objectLoader.loadObject("DIRAC.ResourceStatusSystem.Client.ResourceStatusClient")
        if not res["OK"]:
            self.log.error(f"Failed to load ResourceStatusClient class: {res['Message']}")
            raise ImportError(res["Message"])
        rsClass = res["Value"]

        res = self.objectLoader.loadObject("DIRAC.ResourceStatusSystem.Client.ResourceManagementClient")
        if not res["OK"]:
            self.log.error(f"Failed to load ResourceManagementClient class: {res['Message']}")
            raise ImportError(res["Message"])
        rmClass = res["Value"]

        res = self.objectLoader.loadObject("DIRAC.ResourceStatusSystem.Client.SiteStatus")
        if not res["OK"]:
            self.log.error(f"Failed to load SiteStatus class: {res['Message']}")
            raise ImportError(res["Message"])
        ssClass = res["Value"]

        if "ResourceStatusClient" not in clients:
            self.clients["ResourceStatusClient"] = rsClass()
        if "ResourceManagementClient" not in clients:
            self.clients["ResourceManagementClient"] = rmClass()
        if "SiteStatus" not in clients:
            self.clients["SiteStatus"] = ssClass()

        # Pass to the PDP the clients that are going to be used on the Commands
        self.pdp = PDP(self.clients)

        self.log = gLogger

    def enforce(self, decisionParams):
        """Given a dictionary with decisionParams, it is passed to the PDP, which
        will return ( in case there is a/are positive match/es ) a dictionary containing
        three key-pair values: the original decisionParams ( `decisionParams` ), all
        the policies evaluated ( `singlePolicyResults` ) and the computed final result
        ( `policyCombinedResult` ).

        To know more about decisionParams, please read PDP.setup where the decisionParams
        are sanitized.

        examples:
           >>> pep.enforce( { 'element' : 'Site', 'name' : 'MySite' } )
           >>> pep.enforce( { 'element' : 'Resource', 'name' : 'myce.domain.ch' } )

        :Parameters:
          **decisionParams** - `dict`
            dictionary with the parameters that will be used to match policies.

        """
        if not decisionParams:
            self.log.warn("No decision params...?")
            return S_OK()

        standardParamsDict = {
            "element": None,
            "name": None,
            "elementType": None,
            "statusType": None,
            "status": None,
            "reason": None,
            "tokenOwner": None,
            # Last parameter allows policies to be de-activated
            "active": "Active",
        }

        standardParamsDict.update(decisionParams)

        if standardParamsDict["element"] is not None:
            self.log = gLogger.getSubLogger(f"{self.__class__.__name__}/{standardParamsDict['element']}")
            if standardParamsDict["name"] is not None:
                self.log = gLogger.getSubLogger(
                    f"{self.__class__.__name__}/{standardParamsDict['element']}/{standardParamsDict['name']}"
                )
                self.log.verbose(
                    "Enforce - statusType: %s, status: %s"
                    % (standardParamsDict["statusType"], standardParamsDict["status"])
                )
        decisionParams = dict(standardParamsDict)

        # Setup PDP with new parameters dictionary
        self.pdp.setup(decisionParams)

        # Run policies, get decision, get actions to apply
        resDecisions = self.pdp.takeDecision()
        if not resDecisions["OK"]:
            self.log.error("Something went wrong, not enforcing policies", f"{decisionParams}")
            return resDecisions
        resDecisions = resDecisions["Value"]

        # We take from PDP the decision parameters used to find the policies
        decisionParams = resDecisions["decisionParams"]
        policyCombinedResult = resDecisions["policyCombinedResult"]
        singlePolicyResults = resDecisions["singlePolicyResults"]

        # We have run the actions and at this point, we are about to execute the actions.
        # One more final check before proceeding
        isNotUpdated = self.__isNotUpdated(decisionParams)
        if not isNotUpdated["OK"]:
            return isNotUpdated

        for policyActionName, policyActionType in policyCombinedResult["PolicyAction"]:
            result = self.objectLoader.loadObject(f"DIRAC.ResourceStatusSystem.PolicySystem.Actions.{policyActionType}")
            if not result["OK"]:
                self.log.error(result["Message"])
                continue

            actionClass = result["Value"]
            actionObj = actionClass(
                policyActionName, decisionParams, policyCombinedResult, singlePolicyResults, self.clients
            )

            self.log.debug((policyActionName, policyActionType))

            actionResult = actionObj.run()
            if not actionResult["OK"]:
                self.log.error(actionResult["Message"])

        return S_OK(resDecisions)

    def __isNotUpdated(self, decisionParams):
        """Checks for the existence of the element as it was passed to the PEP. It may
        happen that while being the element processed by the PEP an user through the
        web interface or the CLI has updated the status for this particular element. As
        a result, the PEP would overwrite whatever the user had set. This check is not
        perfect, as still an user action can happen while executing the actions, but
        the probability is close to 0. However, if there is an action that takes seconds
        to be executed, this must be re-evaluated. !

        :Parameters:
          **decisionParams** - `dict`
            dictionary with the parameters that will be used to match policies

        :return: S_OK / S_ERROR

        """

        # Copy original dictionary and get rid of one key we cannot pass as kwarg
        selectParams = dict(decisionParams)
        del selectParams["element"]
        del selectParams["active"]

        # We expect to have an exact match. If not, then something has changed and
        # we cannot proceed with the actions.
        if decisionParams["element"] == "Site":
            unchangedRow = self.clients["SiteStatus"].getSiteStatuses([decisionParams["name"]])
        else:
            unchangedRow = self.clients["ResourceStatusClient"].selectStatusElement(
                decisionParams["element"], "Status", **selectParams
            )
        if not unchangedRow["OK"]:
            return unchangedRow

        if not unchangedRow["Value"]:
            msg = f"{selectParams['name']}  ( {selectParams['status']} / {selectParams['statusType']} ) has been updated after PEP started running"
            self.log.error(msg)
            return S_ERROR(msg)

        return S_OK()
