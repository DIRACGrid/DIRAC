""" DIRAC JobDB class is a front-end to the main WMS database containing
  job definitions and status information. It is used in most of the WMS
  components

  The following methods are provided for public usage:

  getJobParameters()

  setJobParameter()
"""

__RCSID__ = "$Id$"

import sys
import operator

from DIRAC.Core.Utilities import DErrno
from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR
from DIRAC.Core.Utilities import Time
from DIRAC.Core.Utilities.DErrno import EWMSSUBM
from DIRAC.ConfigurationSystem.Client.Config import gConfig
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.Base.ElasticDB import ElasticDB as DB
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getDIRACPlatform
from DIRAC.WorkloadManagementSystem.Client.JobState.JobManifest import JobManifest
from DIRAC.ResourceStatusSystem.Client.SiteStatus import SiteStatus

#############################################################################

JOB_STATES = ['Submitting', 'Received', 'Checking', 'Staging', 'Waiting', 'Matched',
              'Running', 'Stalled', 'Done', 'Completed', 'Failed']
JOB_FINAL_STATES = ['Done', 'Completed', 'Failed']


class JobDB(DB):

  def __init__(self):
    """ Standard Constructor
    """

    DB.__init__(self, 'jobelasticdb', 'WorkloadManagement/JobElasticDB')

    self.jobAttributeNames = []

    self.siteClient = SiteStatus()

    self.log.info("==================================================")

#############################################################################
  def getJobParameters(self, jobID, paramList=None):
    """ Get Job Parameters defined for jobID.
      Returns a dictionary with the Job Parameters.
      If parameterList is empty - all the parameters are returned.
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

    result = self.query('jobelasticdb*', query)

    if result['OK']:
      if result['Value']:

        sources = result['Value']['hits']['hits']

        for i in range(0, len(sources)):

          name = sources[i]['_source']['Name']
          value = sources[i]['_source']['Value']

          try:
            resultDict[name] = value.tostring()
          except BaseException:
            resultDict[name] = value

        return S_OK(resultDict)

    else:
      return S_ERROR('JobDB.getJobParameters: failed to retrieve parameters')

#############################################################################
  def setJobParameter(self, jobID, key, value):
    """ Set a parameter specified by name,value pair for the job JobID
    """

    query = {
        "query": {
            "term": {
                "JobID": jobID}}, "script": {
            "inline": "ctx._source.Value = params.value; ctx._source.Name = params.name", "params": {
                "value": value, "name": key}}}
    indexName = self.generateFullIndexName('jobelasticdb')
    result = self.exists(indexName)

    if result:
      pass

    else:
      mapping = {
          "JobParameters": {
              "properties": {
                  "JobID": {
                      "type": "long"}, "Name": {
                      "type": "text"}, "Value": {
                      "type": "binary"}}}}
      result = self.createIndex('jobelasticdb', mapping)

      if not result['OK']:
        return(result)

    result = self.update('jobelasticdb*', 'JobParameters', query)

    if not result['OK']:
      return S_ERROR('JobDB.setJobParameter: operation failed.')

    if result['Value']['updated'] == 0:

      query = {"JobID": jobID, "Name": key, "Value": value}
      result = self.update(indexName, 'JobParameters', query, update_by_query=False)

    if not result['OK']:
      result = S_ERROR('JobDB.setJobParameter: operation failed.')

    return result
