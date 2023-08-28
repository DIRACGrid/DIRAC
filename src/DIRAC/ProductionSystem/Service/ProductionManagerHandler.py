""" DISET request handler base class for the ProductionDB.
"""

from DIRAC import S_ERROR, S_OK, gLogger
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities.DEncode import ignoreEncodeWarning
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader

prodTypes = [str, int]
transTypes = [str, int, list]


class ProductionManagerHandlerMixin:
    @classmethod
    def initializeHandler(cls, serviceInfoDict):
        """Initialization of DB object"""

        try:
            result = ObjectLoader().loadObject("ProductionSystem.DB.ProductionDB", "ProductionDB")
            if not result["OK"]:
                return result
            cls.productionDB = result["Value"]()
        except RuntimeError as excp:
            return S_ERROR(f"Can't connect to DB: {excp}")

        return S_OK()

    ####################################################################
    #
    # These are the methods to manipulate the Productions table
    #

    types_addProduction = [prodTypes, str]

    def export_addProduction(self, prodName, prodDescription):
        credDict = self.getRemoteCredentials()
        author = credDict.get("username", credDict.get("DN", credDict.get("CN")))
        authorGroup = credDict.get("group")
        res = self.productionDB.addProduction(prodName, prodDescription, author, authorGroup)
        if res["OK"]:
            gLogger.info("Added production", res["Value"])
        return res

    types_deleteProduction = [prodTypes]

    def export_deleteProduction(self, prodName):
        credDict = self.getRemoteCredentials()
        author = credDict.get("username")
        return self.productionDB.deleteProduction(prodName, author=author)

    types_getProductions = []

    @classmethod
    def export_getProductions(
        cls,
        condDict=None,
        older=None,
        newer=None,
        timeStamp="CreationDate",
        orderAttribute=None,
        limit=None,
        offset=None,
    ):
        if not condDict:
            condDict = {}
        return cls.productionDB.getProductions(
            condDict=condDict,
            older=older,
            newer=newer,
            timeStamp=timeStamp,
            orderAttribute=orderAttribute,
            limit=limit,
            offset=offset,
        )

    types_getProduction = [prodTypes]

    @classmethod
    def export_getProduction(cls, prodName):
        return cls.productionDB.getProduction(prodName)

    types_getProductionParameters = [prodTypes, [str, list, tuple]]

    @classmethod
    def export_getProductionParameters(cls, prodName, parameters):
        return cls.productionDB.getProductionParameters(prodName, parameters)

    types_setProductionStatus = [prodTypes, str]

    @classmethod
    def export_setProductionStatus(cls, prodName, status):
        return cls.productionDB.setProductionStatus(prodName, status)

    types_startProduction = [prodTypes]

    @classmethod
    @ignoreEncodeWarning
    def export_startProduction(cls, prodName):
        return cls.productionDB.startProduction(prodName)

    ####################################################################
    #
    # These are the methods to manipulate the ProductionTransformations table
    #

    types_addTransformationsToProduction = [prodTypes, transTypes, transTypes]

    @classmethod
    def export_addTransformationsToProduction(cls, prodName, transIDs, parentTransIDs):
        return cls.productionDB.addTransformationsToProduction(prodName, transIDs, parentTransIDs=parentTransIDs)

    types_getProductionTransformations = []

    @classmethod
    def export_getProductionTransformations(
        cls,
        prodName,
        condDict=None,
        older=None,
        newer=None,
        timeStamp="CreationTime",
        orderAttribute=None,
        limit=None,
        offset=None,
    ):
        if not condDict:
            condDict = {}
        return cls.productionDB.getProductionTransformations(
            prodName,
            condDict=condDict,
            older=older,
            newer=newer,
            timeStamp=timeStamp,
            orderAttribute=orderAttribute,
            limit=limit,
            offset=offset,
        )

    ####################################################################
    #
    # These are the methods to manipulate the ProductionSteps table
    #

    types_addProductionStep = [dict]

    @classmethod
    def export_addProductionStep(cls, prodStep):
        stepName = prodStep["name"]
        stepDescription = prodStep["description"]
        stepLongDescription = prodStep["longDescription"]
        stepBody = prodStep["body"]
        stepType = prodStep["stepType"]
        stepPlugin = prodStep["plugin"]
        stepAgentType = prodStep["agentType"]
        stepGroupSize = prodStep["groupsize"]
        stepInputQuery = prodStep["inputquery"]
        stepOutputQuery = prodStep["outputquery"]
        res = cls.productionDB.addProductionStep(
            stepName,
            stepDescription,
            stepLongDescription,
            stepBody,
            stepType,
            stepPlugin,
            stepAgentType,
            stepGroupSize,
            stepInputQuery,
            stepOutputQuery,
        )
        if res["OK"]:
            gLogger.info("Added production step %d" % res["Value"])
        return res

    types_getProductionStep = [int]

    @classmethod
    def export_getProductionStep(cls, stepID):
        return cls.productionDB.getProductionStep(stepID)


class ProductionManagerHandler(ProductionManagerHandlerMixin, RequestHandler):
    pass
