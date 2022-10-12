""" DIRAC Production DB

    Production database is used to collect and serve the necessary information
    in order to automate the task of transformation preparation for high level productions.
"""
# # imports
import json
import threading

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.DB import DB
from DIRAC.ProductionSystem.Utilities.ProdValidator import ProdValidator
from DIRAC.ProductionSystem.Utilities.ProdTransManager import ProdTransManager
from DIRAC.Core.Utilities.List import intListToString

MAX_ERROR_COUNT = 10

#############################################################################


class ProductionDB(DB):
    """ProductionDB class"""

    def __init__(self, dbname=None, dbconfig=None, dbIn=None, parentLogger=None):
        """The standard constructor takes the database name (dbname) and the name of the
        configuration section (dbconfig)
        """
        if not dbname:
            dbname = "ProductionDB"
        if not dbconfig:
            dbconfig = "Production/ProductionDB"

        if not dbIn:
            DB.__init__(self, dbname, dbconfig, parentLogger=parentLogger)

        self.lock = threading.Lock()

        self.prodValidator = ProdValidator()
        self.ProdTransManager = ProdTransManager()

        self.PRODPARAMS = [
            "ProductionID",
            "ProductionName",
            "Description",
            "CreationDate",
            "LastUpdate",
            "AuthorDN",
            "AuthorGroup",
            "Status",
        ]

        self.TRANSPARAMS = ["TransformationID", "ProductionID", "LastUpdate", "InsertedTime"]

        self.TRANSLINKSPARAMS = ["TransformationID", "ParentTransformationID", "ProductionID"]

        self.PRODSTEPSPARAMS = [
            "StepID",
            "Name",
            "Description",
            "LongDescription",
            "Body",
            "Type",
            "Plugin",
            "AgentType",
            "GroupSize",
            "InputQuery",
            "OutputQuery",
            "LastUpdate",
            "InsertedTime",
        ]

        self.statusActionDict = {
            "New": None,
            "Active": "startTransformation",
            "Stopped": "stopTransformation",
            "Completed": "completeTransformation",
            "Cleaned": "cleanTransformation",
        }

    def addProduction(self, prodName, prodDescription, authorDN, authorGroup, connection=False):
        """Create new production starting from its description

        :param str prodName: a string with the Production name
        :param str prodDescription: a json object with the Production description
        :param str authorDN: string with the author DN
        :param str authorGroup: string with author group
        """
        connection = self.__getConnection(connection)
        res = self._getProductionID(prodName, connection=connection)
        if res["OK"]:
            return S_ERROR("Production with name %s already exists with ProductionID = %d" % (prodName, res["Value"]))
        elif res["Message"] != "Production does not exist":
            return res

        self.lock.acquire()

        req = (
            "INSERT INTO Productions (ProductionName,Description,CreationDate,LastUpdate,\
                                    AuthorDN,AuthorGroup,Status)\
                                VALUES ('%s','%s',UTC_TIMESTAMP(),UTC_TIMESTAMP(),'%s','%s','New');"
            % (prodName, prodDescription, authorDN, authorGroup)
        )

        res = self._update(req, conn=connection)
        if not res["OK"]:
            self.lock.release()
            return res
        prodID = int(res["lastRowId"])
        self.lock.release()

        return S_OK(prodID)

    def getProductions(
        self,
        condDict=None,
        older=None,
        newer=None,
        timeStamp="LastUpdate",
        orderAttribute=None,
        limit=None,
        offset=None,
        connection=False,
    ):
        """Get parameters of all the Productions with support for the web standard structure

        :param dict condDict:
        :param str older:
        :param str newer:
        :param str timeStamp:
        :param str orderAttribute:
        :param int limit:
        :param int offset:
        :param bool connection:
        :return:
        """

        connection = self.__getConnection(connection)
        req = "SELECT {} FROM Productions {}".format(
            intListToString(self.PRODPARAMS),
            self.buildCondition(condDict, older, newer, timeStamp, orderAttribute, limit, offset=offset),
        )
        res = self._query(req, conn=connection)
        if not res["OK"]:
            return res

        webList = []
        resultList = []
        for row in res["Value"]:
            # TODO: remove, as Description should have been converted to a text type
            row = [item.decode() if isinstance(item, bytes) else item for item in row]
            # Prepare the structure for the web
            rList = [str(item) if not isinstance(item, int) else item for item in row]
            prodDict = dict(zip(self.PRODPARAMS, row))
            webList.append(rList)
            resultList.append(prodDict)
        result = S_OK(resultList)
        result["Records"] = webList
        result["ParameterNames"] = self.PRODPARAMS
        return result

    def getProduction(self, prodName, connection=False):
        """Get the Production definition

        :param str prodName: the Production name or ID
        """
        res = self._getConnectionProdID(connection, prodName)
        if not res["OK"]:
            return res
        connection = res["Value"]["Connection"]
        prodID = res["Value"]["ProductionID"]
        res = self.getProductions(condDict={"ProductionID": prodID}, connection=connection)
        if not res["OK"]:
            return res
        if not res["Value"]:
            return S_ERROR("Production %s did not exist" % prodName)
        return S_OK(res["Value"][0])

    def getProductionParameters(self, prodName, parameters, connection=False):
        """Get the requested parameters for a supplied production

        :param str prodName: the Production name or ID
        :param str parameters: any valid production parameter in self.PRODPARAMS
        """
        if isinstance(parameters, str):
            parameters = [parameters]
        res = self.getProduction(prodName, connection=connection)
        if not res["OK"]:
            return res
        prodParams = res["Value"]
        paramDict = {}
        for reqParam in parameters:
            if reqParam not in prodParams:
                return S_ERROR("Parameter %s not defined for production" % reqParam)
            paramDict[reqParam] = prodParams[reqParam]
        if len(paramDict) == 1:
            return S_OK(paramDict[reqParam])
        return S_OK(paramDict)

    ###########################################################################
    #
    # These methods manipulate the ProductionSteps table
    #

    def getProductionStep(self, stepID, connection=False):
        """It returns the ProductionStep corresponding to the stepID

        :param int stepID: the ID of the production Step stored in the ProductionSteps table
        :return: the attributes of Production Step corresponding to the stepID
        """
        connection = self.__getConnection(connection)
        req = f"SELECT {intListToString(self.PRODSTEPSPARAMS)} FROM ProductionSteps WHERE StepID = {str(stepID)}"
        res = self._query(req, conn=connection)
        if not res["OK"]:
            return res
        if not res["Value"]:
            return S_ERROR("ProductionStep %s did not exist" % str(stepID))
        row = res["Value"][0]
        # TODO: remove, as Description and body should have been converted to a text type
        row = [item.decode() if isinstance(item, bytes) else item for item in row]
        return S_OK(row)

    def addProductionStep(
        self,
        stepName,
        stepDescription,
        stepLongDescription,
        stepBody,
        stepType,
        stepPlugin,
        stepAgentType,
        stepGroupSize,
        stepInputquery,
        stepOutputquery,
        connection=False,
    ):
        """Add a Production Step

        :param str stepName: name of the production Step
        :param str stepDescription: description of the production Step
        :param str stepLongDescription: long description of the production Step
        :param str stepBody: body of the production Step
        :param str stepType: type of the production Step
        :param str stepPlugin: plugin to be used for the production Step
        :param str stepAgentType: agent type to be used for the production Step
        :param int stepGroupSize: group size of the production Step
        :param str stepInputquery: InputQuery of the production Step
        :param str stepOutputquery: OutputQuery of the production Step
        :return:
        """
        connection = self.__getConnection(connection)
        self.lock.acquire()
        req = (
            "INSERT INTO ProductionSteps (Name,Description,LongDescription,Body,Type,Plugin,AgentType,GroupSize,\
                                    InputQuery,OutputQuery,LastUpdate,InsertedTime)\
                                VALUES ('%s','%s', '%s', %s, '%s', '%s', '%s', '%s', '%s', '%s',\
                                UTC_TIMESTAMP(),UTC_TIMESTAMP());"
            % (
                stepName,
                stepDescription,
                stepLongDescription,
                stepBody,
                stepType,
                stepPlugin,
                stepAgentType,
                stepGroupSize,
                stepInputquery,
                stepOutputquery,
            )
        )

        res = self._update(req, conn=connection)
        if not res["OK"]:
            self.lock.release()
            return res
        stepID = int(res["lastRowId"])
        self.lock.release()

        return S_OK(stepID)

        ###########################################################################

    #
    # These methods manipulate the ProductionTransformations table
    #

    def getProductionTransformations(
        self,
        prodName,
        condDict=None,
        older=None,
        newer=None,
        timeStamp="CreationTime",
        orderAttribute=None,
        limit=None,
        offset=None,
        connection=False,
    ):
        """Gets the transformations of a given Production

        :param str prodName: the Production name or ID
        :return:
        """
        res = self._getConnectionProdID(connection, prodName)
        if not res["OK"]:
            return res
        connection = res["Value"]["Connection"]
        prodID = res["Value"]["ProductionID"]
        condDict = {"ProductionID": prodID}

        req = "SELECT {} FROM ProductionTransformations {}".format(
            intListToString(self.TRANSPARAMS),
            self.buildCondition(condDict, older, newer, timeStamp, orderAttribute, limit, offset=offset),
        )
        res = self._query(req, conn=connection)
        if not res["OK"]:
            return res

        webList = []
        resultList = []
        for row in res["Value"]:
            # Prepare the structure for the web
            rList = [str(item) if not isinstance(item, int) else item for item in row]
            transDict = dict(zip(self.TRANSPARAMS, row))
            webList.append(rList)
            resultList.append(transDict)
        result = S_OK(resultList)
        result["Records"] = webList
        result["ParameterNames"] = self.TRANSPARAMS
        return result

    def __setProductionStatus(self, prodID, status, connection=False):
        """Set the Status to the Production

        :param int prodID: the ProductionID
        :param str status: the Production status
        """
        req = "UPDATE Productions SET Status='%s', LastUpdate=UTC_TIMESTAMP() WHERE ProductionID=%d" % (status, prodID)
        return self._update(req, conn=connection)

    # This is to be replaced by startProduction, stopProduction etc.
    def setProductionStatus(self, prodName, status, connection=False):
        """Set the status to the production and to all the associated transformations

        :param str prodName: the Production name or ID
        :param str status: the Production status
        """
        res = self._getConnectionProdID(connection, prodName)
        gLogger.error(res)
        if not res["OK"]:
            return res
        connection = res["Value"]["Connection"]
        prodID = res["Value"]["ProductionID"]

        res = self.__setProductionStatus(prodID, status, connection=connection)
        if not res["OK"]:
            return res

        res = self.ProdTransManager.executeActionOnTransformations(prodID, self.statusActionDict[status])
        if not res["OK"]:
            gLogger.error(res["Message"])

        return S_OK()

    def startProduction(self, prodName, connection=False):
        """Instantiate and start the transformations belonging to the production

        :param str prodName: the Production name or ID
        """

        res = self._getConnectionProdID(connection, prodName)
        gLogger.error(res)
        if not res["OK"]:
            return res
        connection = res["Value"]["Connection"]
        prodID = res["Value"]["ProductionID"]

        # Instantiate the transformations according to the description
        res = self.getProductionParameters(prodName, "Description")
        if not res["OK"]:
            return res

        prodDescription = json.loads(res["Value"])
        transIDs = []
        for step in sorted(prodDescription):
            res = self.ProdTransManager.addTransformationStep(prodDescription[step]["stepID"], prodID)
            # If the addition of at least one step failes, clean the already added transformation steps from the TS
            if not res["OK"]:
                self.ProdTransManager.deleteTransformations(transIDs)
                return S_ERROR(res["Message"])
            transID = res["Value"]
            prodDescription[step]["transID"] = transID
            transIDs.append(transID)

        for step in prodDescription:
            transID = prodDescription[step]["transID"]
            parentTransIDs = []
            if "parentStep" in prodDescription[step]:
                for parentStep in prodDescription[step]["parentStep"]:
                    parentTransID = prodDescription[parentStep]["transID"]
                    parentTransIDs.append(parentTransID)

            res = self.addTransformationsToProduction(prodID, transID, parentTransIDs=parentTransIDs)
            # If adding the transformations to production fails, clean all the transformation steps from the TS
            if not res["OK"]:
                self.ProdTransManager.deleteTransformations(transIDs)
                # Clean the production
                self.deleteProduction(prodID)
                return S_ERROR(res["Message"])

        res = self.__setProductionStatus(prodID, "Active", connection=connection)
        if not res["OK"]:
            return res

        res = self.ProdTransManager.executeActionOnTransformations(prodID, self.statusActionDict["Active"])
        if not res["OK"]:
            gLogger.error(res["Message"])

        return S_OK()

    def __deleteProduction(self, prodID, connection=False):
        """Delete a given production from the Productions table

        :param int prodID: ProductionID
        """
        req = "DELETE FROM Productions WHERE ProductionID=%d;" % prodID
        return self._update(req, conn=connection)

    def __deleteProductionTransformations(self, prodID, connection=False):
        """Remove all the transformations of the specified production from the TS and from the PS

        :param int prodID: the ProductionID
        """

        # Remove transformations from the TS
        gLogger.notice("Deleting transformations of Production %s from the TS" % prodID)
        res = self.ProdTransManager.deleteProductionTransformations(prodID)
        if not res["OK"]:
            gLogger.error("Failed to delete production transformations from the TS", res["Message"])

        # Remove transformations from the PS
        req = "DELETE FROM ProductionTransformationLinks WHERE ProductionID = %d;" % prodID
        res = self._update(req, conn=connection)
        if not res["OK"]:
            gLogger.error("Failed to delete production transformation links from the PS", res["Message"])

        req = "DELETE FROM ProductionTransformations WHERE ProductionID = %d;" % prodID
        res = self._update(req, conn=connection)
        if not res["OK"]:
            gLogger.error("Failed to delete production transformations from the PS", res["Message"])

        return res

    def cleanProduction(self, prodName, author="", connection=False):
        """Clean the production specified by name or id

        :param str prodName: the Production name or ID
        """
        res = self._getConnectionProdID(connection, prodName)
        if not res["OK"]:
            return res
        connection = res["Value"]["Connection"]
        prodID = res["Value"]["ProductionID"]
        res = self.__deleteProductionTransformations(prodID, connection=connection)
        if not res["OK"]:
            return res

        return S_OK(prodID)

    def deleteProduction(self, prodName, author="", connection=False):
        """Remove the production specified by name or id

        :param str prodName: the Production name or ID
        """
        res = self._getConnectionProdID(connection, prodName)
        gLogger.error(res)
        if not res["OK"]:
            return res
        connection = res["Value"]["Connection"]
        prodID = res["Value"]["ProductionID"]
        res = self.cleanProduction(prodID, author=author, connection=connection)
        if not res["OK"]:
            return res
        res = self.__deleteProduction(prodID, connection=connection)
        if not res["OK"]:
            return res

        return S_OK()

    def addTransformationsToProduction(self, prodName, transIDs, parentTransIDs=None, connection=False):
        """Check the production validity and the add the transformations to the production

        :param str prodName: the Production name or ID
        :param list transIDs: the list of transformations to be added to the production
        """
        gLogger.info(
            "ProductionDB.addTransformationsToProduction: \
         Attempting to add %s transformations to production: %s"
            % (transIDs, prodName)
        )

        if not transIDs:
            return S_ERROR("Zero length transformation list")
        res = self._getConnectionProdID(connection, prodName)
        if not res["OK"]:
            return res
        connection = res["Value"]["Connection"]
        prodID = res["Value"]["ProductionID"]

        # Check the production validity
        gLogger.notice("Checking if production is valid")

        # Set transformations in list format
        if parentTransIDs:
            if not isinstance(parentTransIDs, list):
                parentTransIDs = [parentTransIDs]

        if not isinstance(transIDs, list):
            transIDs = [transIDs]

        # Check the status of the transformations (must be 'New')
        for transID in transIDs:
            res = self.prodValidator.checkTransStatus(transID)
            if not res["OK"]:
                gLogger.error("Production is not valid:", res["Message"])
                return res

        # Check if transformations are linked to their parent transformations, if they have one(s)
        if parentTransIDs:
            for transID in transIDs:
                for parentTransID in parentTransIDs:
                    gLogger.notice(
                        "Checking if transformation %s is linked to the parent transformation %s"
                        % (transID, parentTransID)
                    )
                    res = self.prodValidator.checkTransDependency(transID, parentTransID)
                    if not res["OK"]:
                        gLogger.error("Production is not valid:", res["Message"])
                        return res

        gLogger.notice("Production %s is valid" % prodName)

        res = self.__addTransformations(prodID, transIDs, connection=connection)
        if not res["OK"]:
            msg = "Failed to add transformations {} to production {}: {}".format(transIDs, prodID, res["Message"])
            return S_ERROR(msg)
        # Add the transformation links (tranformation to parent transformation) for the given production
        res = self.__addTransformationLinks(prodID, transIDs, parentTransIDs=parentTransIDs, connection=connection)
        if not res["OK"]:
            msg = "Failed to add production transformations links to transformations {}: {}".format(
                transIDs,
                res["Message"],
            )
            return S_ERROR(msg)

        # Update the status of the transformation to be in sync with the status of the production
        res = self.getProduction(prodID)
        prodStatus = res["Value"]["Status"]

        # Execute action on transformations according to the production status
        res = self.ProdTransManager.executeActionOnTransformations(prodID, self.statusActionDict[prodStatus])
        if not res["OK"]:
            gLogger.error(res["Message"])

        return S_OK()

    def __addTransformationLinks(self, prodID, transIDs, parentTransIDs=None, connection=False):
        """Insert the transformations in the ProductionTransformationLinks table

        :param prodName: the Production name or ID
        :param transIDs: the list of transformations to be associated ProductionTransformationLinks table
        """
        req = "INSERT INTO ProductionTransformationLinks \
           (TransformationID,ParentTransformationID,ProductionID) VALUES"

        # Insert transformations and the corresponding parent transformations
        if parentTransIDs:
            for transID in transIDs:
                for parentTransID in parentTransIDs:
                    req = "%s (%d,%d,%d)," % (req, transID, parentTransID, prodID)
        # If parent transformations are not defined, just insert transformations and use the parent transformation default
        # value -1
        else:
            req = "INSERT INTO ProductionTransformationLinks (TransformationID,ProductionID) VALUES"
            for transID in transIDs:
                req = "%s (%d,%d)," % (req, transID, prodID)

        gLogger.notice(req)
        req = req.rstrip(",")
        res = self._update(req, conn=connection)
        if not res["OK"]:
            return res

        return S_OK()

    def __addTransformations(self, prodID, transIDs, connection=False):
        """Insert the transformations in the ProductionTransformations table

        :param str prodName: the Production name or ID
        :param list transIDs: the list of transformations to be added to the production
        """
        req = "INSERT INTO ProductionTransformations \
           (ProductionID,TransformationID,LastUpdate,InsertedTime) VALUES"
        for transID in transIDs:
            req = "%s (%d,%d,UTC_TIMESTAMP(),UTC_TIMESTAMP())," % (req, prodID, transID)
            gLogger.notice(req)
        req = req.rstrip(",")
        res = self._update(req, conn=connection)
        if not res["OK"]:
            return res

        return S_OK()

    def _getProductionID(self, prodName, connection=False):
        """Method returns ID of production specified by the prodName

        :param str prodName: the Production name
        """
        try:
            prodName = int(prodName)
            cmd = "SELECT ProductionID from Productions WHERE ProductionID=%d;" % prodName
        except Exception:
            if not isinstance(prodName, str):
                return S_ERROR("Production should be ID or name")
            cmd = "SELECT ProductionID from Productions WHERE ProductionName='%s';" % prodName
        res = self._query(cmd, conn=connection)
        if not res["OK"]:
            gLogger.error("Failed to obtain production ID for production", "{}: {}".format(prodName, res["Message"]))
            return res
        elif not res["Value"]:
            gLogger.verbose("Production %s does not exist" % (prodName))
            return S_ERROR("Production does not exist")
        return S_OK(res["Value"][0][0])

    def __getConnection(self, connection):
        """Get the MySQL connection

        :param bool connection: the DB connection
        """
        if connection:
            return connection
        res = self._getConnection()
        if res["OK"]:
            return res["Value"]
        gLogger.warn("Failed to get MySQL connection", res["Message"])
        return connection

    def _getConnectionProdID(self, connection, prodName):
        """Get the Production ID for a given production

        :param bool connection: the DB connection
        :param prodName: the Production name
        """
        connection = self.__getConnection(connection)
        res = self._getProductionID(prodName, connection=connection)
        if not res["OK"]:
            gLogger.error("Failed to get ID for production {}: {}".format(prodName, res["Message"]))
            return res
        prodID = res["Value"]
        resDict = {"Connection": connection, "ProductionID": prodID}
        return S_OK(resDict)
