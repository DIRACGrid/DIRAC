""" Class that contains client access to the production DB handler. """
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.Client import Client, createClient
from DIRAC.ProductionSystem.Utilities.StateMachine import ProductionsStateMachine


@createClient("Production/ProductionManager")
class ProductionClient(Client):
    """Exposes the functionality available in the ProductionSystem/ProductionManagerHandler"""

    def __init__(self, **kwargs):
        """Simple constructor"""

        super().__init__(**kwargs)
        self.setServer("Production/ProductionManager")
        self.prodDescription = {}
        self.stepCounter = 1

    # Method applying the Production System State machine

    def _applyProductionStatusStateMachine(self, prodID, status, force=False):
        """Performs a state machine check for productions when asked to change the status

        :param int prodID: the ProductionID on which the state machine check is performed
        :param str status: the proposed status which is checked to be valid
        :param bool force: a boolean. When force=True the proposed status is forced to pass the state machine check

        :return: S_OK with the new status or S_ERROR
        """
        res = self.getProductionParameters(prodID, "Status")
        if not res["OK"]:
            return res
        originalStatus = res["Value"]
        proposedStatus = status
        if force:
            return S_OK(proposedStatus)
        else:
            stateChange = ProductionsStateMachine(originalStatus).setState(proposedStatus)
            if not stateChange["OK"]:
                return S_OK(originalStatus)
            else:
                return S_OK(stateChange["Value"])

    # Methods contacting the ProductionManager Service

    def setProductionStatus(self, prodID, status):
        """Sets the status of a production

        :param int prodID: the ProductionID
        :param str status: the production status to be set to the prodID
        """
        rpcClient = self._getRPC()
        # Apply the production state machine
        res = self._applyProductionStatusStateMachine(prodID, status, force=False)
        if not res["OK"]:
            return res
        newStatus = res["Value"]
        if newStatus != status:
            return S_ERROR("Cannot change status")
        return rpcClient.setProductionStatus(prodID, status)

    def getProductions(self, condDict=None, older=None, newer=None, timeStamp=None, orderAttribute=None, limit=100):
        """Gets all the productions in the system, incrementally. "limit" here is just used to determine the offset."""

        rpcClient = self._getRPC()
        productions = []
        if condDict is None:
            condDict = {}
        if timeStamp is None:
            timeStamp = "CreationDate"
        # getting transformations - incrementally
        offsetToApply = 0
        while True:
            res = rpcClient.getProductions(condDict, older, newer, timeStamp, orderAttribute, limit, offsetToApply)
            if not res["OK"]:
                return res
            else:
                gLogger.verbose("Result for limit %d, offset %d: %d" % (limit, offsetToApply, len(res["Value"])))
                if res["Value"]:
                    productions = productions + res["Value"]
                    offsetToApply += limit
                if len(res["Value"]) < limit:
                    break
        return S_OK(productions)

    def getProductionTransformations(
        self, prodName, condDict=None, older=None, newer=None, timeStamp=None, orderAttribute=None, limit=10000
    ):
        """Gets all the production transformations for a production, incrementally.
            "limit" here is just used to determine the offset.

        :param str prodName: the production name
        :return: the list of the transformations associated to the production
        """

        rpcClient = self._getRPC()
        productionTransformations = []

        if condDict is None:
            condDict = {}
        if timeStamp is None:
            timeStamp = "CreationTime"
        # getting productionTransformations - incrementally
        offsetToApply = 0
        while True:
            res = rpcClient.getProductionTransformations(
                prodName, condDict, older, newer, timeStamp, orderAttribute, limit, offsetToApply
            )
            if not res["OK"]:
                return res
            else:
                gLogger.verbose("Result for limit %d, offset %d: %d" % (limit, offsetToApply, len(res["Value"])))
                if res["Value"]:
                    productionTransformations = productionTransformations + res["Value"]
                    offsetToApply += limit
                if len(res["Value"]) < limit:
                    break
        return S_OK(productionTransformations)

    def addProductionStep(self, prodStep):
        """Add a production step and update the production description

        :param object prodStep: the production step, i.e. a ProductionStep object describing the transformation
        """
        stepName = "Step" + str(self.stepCounter) + "_" + prodStep.Name
        self.stepCounter += 1
        prodStep.Name = stepName

        res = prodStep.getAsDict()
        if not res["OK"]:
            return res
        prodStepDict = res["Value"]
        rpcClient = self._getRPC()
        res = rpcClient.addProductionStep(prodStepDict)
        if not res["OK"]:
            return res
        stepID = res["Value"]
        self.prodDescription[prodStep.Name] = {"stepID": stepID, "parentStep": prodStepDict["parentStep"]}
        return S_OK()
