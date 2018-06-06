""" DIRAC JobDB class is a front-end to the main WMS database containing
  job definitions and status information. It is used in most of the WMS
  components

  The following methods are provided for public usage:

  getJobParameters()

  setJobParameter()
"""

__RCSID__ = "$Id$"

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Base.ElasticDB import ElasticDB as DB

#############################################################################


class ElasticJobDB(DB):

  def __init__(self):
    """ Standard Constructor
    """

    DB.__init__(self, 'jobelasticdb', 'WorkloadManagement/ElasticJobDB')

    self.log.info("==================================================")

#############################################################################
  def getJobParameters(self, jobID, paramList=None):
    """ Get Job Parameters defined for jobID.
      Returns a dictionary with the Job Parameters.
      If paramList is empty - all the parameters are returned.

    :param self: self reference
    :param int jobID: Job ID
    :param list paramList: list of parameters to be returned
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

    gLogger.debug("Getting results for ", jobID)
    result = self.query('jobelasticdb*', query)

    if not result['OK']:
      return S_ERROR(result)

    sources = result['Value']['hits']['hits']

    for source in sources:

      name = source['_source']['Name']
      value = source['_source']['Value']

      try:
        resultDict[name] = value.tostring()
      except BaseException:
        resultDict[name] = value

    return S_OK(resultDict)

#############################################################################
  def setJobParameter(self, jobID, key, value):

    """ Set a parameter specified by name,value pair for the job JobID

    :param self: self reference
    :param int jobID: Job ID
    :param basestring key: Name
    :param keyword value: value
    """

    query = {
        "query": {
            "term": {
                "JobID": jobID}}, "script": {
            "inline": "ctx._source.Value = params.value; ctx._source.Name = params.name", "params": {
                "value": value, "name": key}}}
    indexName = self.generateFullIndexName('jobelasticdb')
    result = self.exists(indexName)

    if not result:
      mapping = {
          "JobParameters": {
              "properties": {
                  "JobID": {
                      "type": "long"}, "Name": {
                      "type": "text"}, "Value": {
                      "type": "keyword"}}}}

      gLogger.debug("Creating index ", indexName)
      result = self.createIndex('jobelasticdb', mapping)

      if not result['OK']:
        return result

    result = self.update('jobelasticdb*', 'JobParameters', query)

    if not result['OK']:
      gLogger.error("JobDB.setJobParameter: operation failed.")
      return result

    if result['Value']['updated'] == 0:

      gLogger.debug("Updated values: ", 0)
      query = {"JobID": jobID, "Name": key, "Value": value}

      gLogger.debug("Inserting values in index ", indexName)
      result = self.update(indexName, 'JobParameters', query, updateByQuery=False, id=jobID)

    if not result['OK']:
      gLogger.error("JobDB.setJobParameter: operation failed.")
      return result

    return result
