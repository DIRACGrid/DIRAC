""" DIRAC JobDB class is a front-end to the main WMS database containing
  job definitions and status information. It is used in most of the WMS
  components

  The following methods are provided for public usage:

  getJobParameters()
  getJobParametersAndAttributes()
  setJobParameter()
"""

from __future__ import absolute_import
__RCSID__ = "$Id$"

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Base.ElasticDB import ElasticDB as DB

indexName = 'jobelasticdb'


class ElasticJobDB(DB):

  def __init__(self):
    """ Standard Constructor
    """

    DB.__init__(self, indexName, 'WorkloadManagement/ElasticJobDB')

    self.log.info("==================================================")

  def getJobParameters(self, jobID, paramList=None):
    """ Get Job Parameters defined for jobID.
      Returns a dictionary with the Job Parameters.
      If paramList is empty - all the parameters are returned.

    :param self: self reference
    :param int jobID: Job ID
    :param list paramList: list of parameters to be returned

    :return: dict with all Job Parameter values
    """

    self.log.debug('JobDB.getParameters: Getting Parameters for job %s' % jobID)

    resultDict = {}

    if paramList:
      paramNameList = []

      for x in paramList:
        paramNameList.append(x)

      query = {
          "query": {
              "bool": {
                  "must": [
                      {"match": {"JobID": jobID}}, {"match": {"Name": ','.join(paramNameList)}}]}},
          "_source": ["Name", "Value"]}

    else:
      query = {"query": {"match": {"JobID": jobID}}, "_source": ["Name", "Value"]}

    result = self.query(indexName, query)

    if not result['OK']:
      return result

    sources = result['Value']['hits']['hits']

    for source in sources:

      name = source['_source']['Name']
      value = source['_source']['Value']

      try:
        resultDict[name] = value.tostring()
      except BaseException:
        resultDict[name] = value

    return S_OK({jobID: resultDict})

  def getJobParametersAndAttributes(self, jobID, attribute=None, paramList=None):
    """ Get Job Parameters with Attributes defined for jobID.
      Returns a dictionary with the Job Parameters.
      If paramList is empty - all the parameters are returned.

    :param self: self reference
    :param int jobID: Job ID
    :param string attribute: Attribute
    :param list paramList: list of parameters to be returned

    :return: dict with all Job Parameter and Attribute values
    """

    self.log.debug('JobDB.getJobParametersAndAttributes: Getting Parameters and attributes for job %d' % jobID)

    jobParameters = ["JobID", "Name", "Value", "JobGroup", "Owner", "Proxy", "SubmissionTime", "RunningTime"]
    resultDict = {}

    if paramList:

      query = {
          "query": {
              "bool": {
                  "must": [
                      {"match": {"JobID": jobID}}, {"match": {"Name": ','.join(paramList)}}]}},
          "_source": jobParameters}

    else:
      query = {"query": {"match": {"JobID": jobID}}, "_source": jobParameters}

    gLogger.debug("Getting results for %d" % jobID)
    result = self.query(indexName, query)

    if not result['OK']:
      return result

    sources = result['Value']['hits']['hits']
    jobParameters.remove("JobID")

    for source in sources:
      jobID = source['_source']['JobID']
      parametersDict = {}

      for parameter in jobParameters:
        parametersDict[parameter] = source['_source'][parameter]

      resultDict[jobID] = parametersDict

    if attribute:
      if attribute in jobParameters:
        return S_OK(resultDict[jobID][attribute])
      else:
        return S_ERROR('Attribute %s not found' % attribute)

    else:
      return S_OK(resultDict)

  def setJobParameter(self, jobID, key, value, **kwargs):
    """
    Inserts data into specified index using data given in argument

    :returns: S_OK/S_ERROR as result of indexing
    """
    typeName = 'JobParameters'

    data = {"JobID": jobID,
            "Name": key,
            "Value": value}

    attributesDict = {"JobGroup": "00000000", "Owner": 'Unknown', "Proxy": None,
                      "SubmissionTime": None, "RunningTime": None}
    attributesDict.update(kwargs)

    data.update(attributesDict)

    self.log.debug('Inserting data in %s:%s' % (indexName, typeName))
    self.log.debug(data)

    result = self.index(indexName, typeName, data, id=str(jobID) + key)
    if not result['OK']:
      self.log.error("ERROR: Couldn't insert data", result['Message'])
    return result
