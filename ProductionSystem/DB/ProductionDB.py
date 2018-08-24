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


__RCSID__ = "$Id$"

MAX_ERROR_COUNT = 10

#############################################################################


class ProductionDB(DB):
  """ ProductionDB class
  """

  def __init__(self, dbname=None, dbconfig=None, dbIn=None):
    """ The standard constructor takes the database name (dbname) and the name of the
        configuration section (dbconfig)
    """
    if not dbname:
      dbname = 'ProductionDB'
    if not dbconfig:
      dbconfig = 'Production/ProductionDB'

    if not dbIn:
      DB.__init__(self, dbname, dbconfig)

    self.lock = threading.Lock()

    self.prodValidator = ProdValidator()
    self.ProdTransManager = ProdTransManager()

    self.PRODPARAMS = ['ProductionID',
                       'ProductionName',
                       'Description',
                       'CreationDate',
                       'LastUpdate',
                       'AuthorDN',
                       'AuthorGroup',
                       'Status']

    self.TRANSPARAMS = ['TransformationID',
                        'ProductionID',
                        'ParentTransformationID',
                        # 'Status',
                        # 'ExternalStatus',
                        'LastUpdate',
                        'InsertedTime']

    self.statusActionDict = {
        'New': None,
        'Active': 'startTransformation',
        'Stopped': 'stopTransformation',
        'Cleaned': 'cleanTransformation'}

  def addProduction(self, prodName, prodDescription, authorDN, authorGroup, connection=False):
    """ Create new production starting from its description
    """
    connection = self.__getConnection(connection)
    res = self._getProductionID(prodName, connection=connection)
    if res['OK']:
      return S_ERROR("Production with name %s already exists with ProductionID = %d" % (prodName,
                                                                                        res['Value']))
    elif res['Message'] != "Production does not exist":
      return res
    self.lock.acquire()

    req = "INSERT INTO Productions (ProductionName, Description, CreationDate,LastUpdate, \
                                    AuthorDN,AuthorGroup,Status)\
                                VALUES ('%s','%s',UTC_TIMESTAMP(),UTC_TIMESTAMP(),'%s','%s','New');" % \
        (prodName, prodDescription, authorDN, authorGroup)
    res = self._update(req, connection)
    if not res['OK']:
      self.lock.release()
      return res
    prodID = res['lastRowId']
    self.lock.release()

    return S_OK(prodID)

  def getProductions(self, condDict=None, older=None, newer=None, timeStamp='LastUpdate',
                     orderAttribute=None, limit=None, offset=None, connection=False):
    """ Get parameters of all the Productions with support for the web standard structure """
    connection = self.__getConnection(connection)
    req = "SELECT %s FROM Productions %s" % (intListToString(self.PRODPARAMS),
                                             self.buildCondition(condDict, older, newer, timeStamp,
                                                                 orderAttribute, limit, offset=offset))
    res = self._query(req, connection)
    if not res['OK']:
      return res

    webList = []
    resultList = []
    for row in res['Value']:
      # Prepare the structure for the web
      rList = [str(item) if not isinstance(item, (long, int)) else item for item in row]
      prodDict = dict(zip(self.PRODPARAMS, row))
      webList.append(rList)
      # if extraParams:
      #  res = self.__getAdditionalParameters( transDict['TransformationID'], connection = connection )
      #  if not res['OK']:
      #    return res
      #  transDict.update( res['Value'] )
      resultList.append(prodDict)
    result = S_OK(resultList)
    result['Records'] = webList
    result['ParameterNames'] = self.PRODPARAMS
    return result

  def getProduction(self, prodName, connection=False):
    """Get Production definition of Production identified by ProductionID
    """
    res = self._getConnectionProdID(connection, prodName)
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    prodID = res['Value']['ProductionID']
    res = self.getProductions(condDict={'ProductionID': prodID},
                              connection=connection)
    if not res['OK']:
      return res
    if not res['Value']:
      return S_ERROR("Production %s did not exist" % prodName)
    return S_OK(res['Value'][0])

  def getProductionParameters(self, prodName, parameters, connection=False):
    """ Get the requested parameters for a supplied production """
    if isinstance(parameters, basestring):
      parameters = [parameters]
    res = self.getProduction(prodName, connection=connection)
    if not res['OK']:
      return res
    prodParams = res['Value']
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
  # These methods manipulate the ProductionTransformations table
  #

  def getProductionTransformations(self, prodName, condDict=None, older=None, newer=None, timeStamp='CreationTime',
                                   orderAttribute=None, limit=None,
                                   offset=None, connection=False):

    res = self._getConnectionProdID(connection, prodName)
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    prodID = res['Value']['ProductionID']
    condDict = {'ProductionID': prodID}

    req = "SELECT %s FROM ProductionTransformations %s" % (intListToString(self.TRANSPARAMS),
                                                           self.buildCondition(condDict, older, newer, timeStamp,
                                                                               orderAttribute, limit, offset=offset))
    res = self._query(req, connection)
    if not res['OK']:
      return res

    webList = []
    resultList = []
    for row in res['Value']:
      # Prepare the structure for the web
      rList = [str(item) if not isinstance(item, (long, int)) else item for item in row]
      transDict = dict(zip(self.TRANSPARAMS, row))
      webList.append(rList)
      resultList.append(transDict)
    result = S_OK(resultList)
    result['Records'] = webList
    result['ParameterNames'] = self.TRANSPARAMS
    return result

  def __setProductionStatus(self, prodID, status, connection=False):
    req = "UPDATE Productions SET Status='%s', LastUpdate=UTC_TIMESTAMP() WHERE ProductionID=%d" % (status, prodID)
    return self._update(req, connection)

# This is to be replaced by startProduction, stopProduction etc.
  def setProductionStatus(self, prodName, status, connection=False):
    """ Set the status to the production specified by name or id and to all the associated transformations"""
    res = self._getConnectionProdID(connection, prodName)
    gLogger.error(res)
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    prodID = res['Value']['ProductionID']

    res = self.__setProductionStatus(prodID, status, connection=connection)
    if not res['OK']:
      return res

    res = self.ProdTransManager.executeActionOnTransformations(prodID, self.statusActionDict[status])
    if not res['OK']:
      gLogger.error(res['Message'])

    return S_OK()

  def startProduction(self, prodName, connection=False):
    """ Instantiate and start the transformations"""
    res = self._getConnectionProdID(connection, prodName)
    gLogger.error(res)
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    prodID = res['Value']['ProductionID']

    # Instantiate the transformations according to the description
    res = self.getProductionParameters(prodName, 'Description')
    if not res['OK']:
      return res
    prodDescription = json.loads(res['Value'])

    for step in prodDescription:
      res = self.ProdTransManager.addTransformationStep(prodDescription[step], prodID)
      if not res['OK']:
        return S_ERROR(res['Message'])
      transID = res['Value']
      prodDescription[step]['transID'] = transID

    for step in prodDescription:
      transID = prodDescription[step]['transID']
      parentTransIDs = []
      if 'parentStep' in prodDescription[step]:
        for parentStep in prodDescription[step]['parentStep']:
          parentTransID = prodDescription[parentStep]['transID']
          parentTransIDs.append(parentTransID)
      else:
        parentTransIDs = [-1]

      res = self.addTransformationsToProduction(prodID, transID, parentTransIDs)
      if not res['OK']:
        return S_ERROR(res['Message'])

    res = self.__setProductionStatus(prodID, 'Active', connection=connection)
    if not res['OK']:
      return res

    res = self.ProdTransManager.executeActionOnTransformations(prodID, self.statusActionDict['Active'])
    if not res['OK']:
      gLogger.error(res['Message'])

    return S_OK()

  def __deleteProduction(self, prodID, connection=False):
    req = "DELETE FROM Productions WHERE ProductionID=%d;" % prodID
    return self._update(req, connection)

  def __deleteProductionTransformations(self, prodID, connection=False):
    ''' Remove all transformations of the production from the TS and from the PS '''

    # Remove transformations from the TS
    gLogger.notice("Deleting transformations of Production %s from the TS" % prodID)
    res = self.ProdTransManager.deleteTransformations(prodID)
    if not res['OK']:
      gLogger.error("Failed to delete production transformations from the TS", res['Message'])

    # Remove transformations from the PS
    req = "DELETE FROM ProductionTransformations WHERE ProductionID = %d;" % prodID
    res = self._update(req, connection)
    if not res['OK']:
      gLogger.error("Failed to delete production transformations from the PS", res['Message'])

    return res

  def cleanProduction(self, prodName, author='', connection=False):
    """ Clean the production specified by name or id """
    res = self._getConnectionProdID(connection, prodName)
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    prodID = res['Value']['ProductionID']
    res = self.__deleteProductionTransformations(prodID, connection=connection)
    if not res['OK']:
      return res

    return S_OK(prodID)

  def deleteProduction(self, prodName, author='', connection=False):
    """ Remove the production specified by name or id """
    res = self._getConnectionProdID(connection, prodName)
    gLogger.error(res)
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    prodID = res['Value']['ProductionID']
    res = self.cleanProduction(prodID, author=author, connection=connection)
    if not res['OK']:
      return res
    res = self.__deleteProduction(prodID, connection=connection)
    if not res['OK']:
      return res

    return S_OK()

  def addTransformationsToProduction(self, prodName, transIDs, parentTransIDs, connection=False):
    """ Add a list of transformations to the production directly.
        The parentTrans must be set (use -1 to set no parentTrans).
    """
    gLogger.info(
        "ProductionDB.addTransformationsToProduction: \
         Attempting to add %s transformations with parentTransIDs %s to production: %s" %
        (transIDs, parentTransIDs, prodName))
    if not transIDs:
      return S_ERROR('Zero length transformation list')
    res = self._getConnectionProdID(connection, prodName)
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    prodID = res['Value']['ProductionID']
    res = self.__addTransformationsToProduction(prodID, transIDs, parentTransIDs, connection=connection)
    if not res['OK']:
      msg = "Failed to add transformations %s to production %s: %s" % (transIDs, prodID, res['Message'])
      return S_ERROR(msg)

    return S_OK()

  def __addTransformationsToProduction(self, prodID, transIDs, parentTransIDs, connection=False):

    # Check if the production definition is valid
    # Do the check only if the parent transformation is defined
    gLogger.notice('Checking if production is valid')
    if not isinstance(parentTransIDs, list):
      parentTransIDs = [parentTransIDs]
    if not isinstance(transIDs, list):
      transIDs = [transIDs]

    # Check the status of the transformation (must be 'New')
    for transID in transIDs:
      res = self.prodValidator.checkTransStatus(transID)
      if not res['OK']:
        gLogger.error("Production is not valid:", res['Message'])
        return res

    if -1 not in parentTransIDs:
      for transID in transIDs:
        for parentTransID in parentTransIDs:
          gLogger.notice(
              'Checking if transformation %s is linked to the parent transformation %s' %
              (transID, parentTransID))
          res = self.prodValidator.checkTransDependency(transID, parentTransID)
          if not res['OK']:
            gLogger.error("Production is not valid:", res['Message'])
            return res

    gLogger.notice('Production is valid')

    req = "INSERT INTO ProductionTransformations \
           (ProductionID,TransformationID,ParentTransformationID,LastUpdate,InsertedTime) VALUES"
    for transID in transIDs:
      req = "%s (%d,%d,'%s',UTC_TIMESTAMP(),UTC_TIMESTAMP())," % (req, prodID, transID, str(parentTransIDs))
      gLogger.notice(req)
    req = req.rstrip(',')
    res = self._update(req, connection)
    if not res['OK']:
      return res

    # Update the status of the transformation to be in sync with the status of the production
    res = self.getProduction(prodID)
    prodStatus = res['Value']['Status']

    res = self.ProdTransManager.executeActionOnTransformations(prodID, self.statusActionDict[prodStatus], transID)
    if not res['OK']:
      gLogger.error(res['Message'])

    return S_OK()

  def _getProductionID(self, prodName, connection=False):
    """ Method returns ID of production with the name=<name> """
    try:
      prodName = long(prodName)
      cmd = "SELECT ProductionID from Productions WHERE ProductionID=%d;" % prodName
    except BaseException:
      if not isinstance(prodName, basestring):
        return S_ERROR("Production should be ID or name")
      cmd = "SELECT ProductionID from Productions WHERE ProductionName='%s';" % prodName
    res = self._query(cmd, connection)
    if not res['OK']:
      gLogger.error("Failed to obtain production ID for production", "%s: %s" % (prodName, res['Message']))
      return res
    elif not res['Value']:
      gLogger.verbose("Production %s does not exist" % (prodName))
      return S_ERROR("Production does not exist")
    return S_OK(res['Value'][0][0])

  def __getConnection(self, connection):
    if connection:
      return connection
    res = self._getConnection()
    if res['OK']:
      return res['Value']
    gLogger.warn("Failed to get MySQL connection", res['Message'])
    return connection

  def _getConnectionProdID(self, connection, prodName):
    connection = self.__getConnection(connection)
    res = self._getProductionID(prodName, connection=connection)
    if not res['OK']:
      gLogger.error("Failed to get ID for production %s: %s" % (prodName, res['Message']))
      return res
    prodID = res['Value']
    resDict = {'Connection': connection, 'ProductionID': prodID}
    return S_OK(resDict)
