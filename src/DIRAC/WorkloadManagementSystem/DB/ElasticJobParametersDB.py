""" Module containing a front-end to the ElasticSearch-based ElasticJobParametersDB.
    This module interacts with one ES index: "ElasticJobParametersDB",
    which is a drop-in replacement for MySQL-based table JobDB.JobParameters.
    While JobDB.JobParameters in MySQL is defined as::

      CREATE TABLE `JobParameters` (
        `JobID` INT(11) UNSIGNED NOT NULL,
        `Name` VARCHAR(100) NOT NULL,
        `Value` BLOB NOT NULL,
        PRIMARY KEY (`JobID`,`Name`),
        FOREIGN KEY (`JobID`) REFERENCES `Jobs`(`JobID`)
      ) ENGINE=InnoDB DEFAULT CHARSET=latin1;

    Here we define a mapping as::

      "JobID": {"type": "long"},
      "Name": {"type": "keyword"},
      "Value": {"type": "text"}

    which is an "equivalent" representation.

    The reason for switching to a ES-based JobParameters lies in the extended searching
    capabilities of ES (ES will analyze+index text fields, while MySQL won't do that on BLOB types).
    This results in higher traceability for DIRAC jobs.

    The following class methods are provided for public usage
      - getJobParameters()
      - setJobParameter()
      - deleteJobParameters()
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import six

__RCSID__ = "$Id$"

from DIRAC import S_OK, gConfig
from DIRAC.ConfigurationSystem.Client.PathFinder import getDatabaseSection
from DIRAC.ConfigurationSystem.Client.Helpers import CSGlobals
from DIRAC.Core.Base.ElasticDB import ElasticDB

name = 'ElasticJobParametersDB'

mapping = {
    "properties": {
        "JobID": {
            "type": "long"
        },
        "Name": {
            "type": "keyword"
        },
        "Value": {
            "type": "text"
        }
    }
}


class ElasticJobParametersDB(ElasticDB):

  def __init__(self):
    """ Standard Constructor
    """

    try:
      section = getDatabaseSection("WorkloadManagement/ElasticJobParametersDB")
      indexPrefix = gConfig.getValue(
          "%s/IndexPrefix" % section, CSGlobals.getSetup()).lower()

      # Connecting to the ES cluster
      super(ElasticJobParametersDB, self).__init__(
          name, 'WorkloadManagement/ElasticJobParametersDB', indexPrefix)
    except Exception as ex:
      self.log.error("Can't connect to ElasticJobParametersDB", repr(ex))
      raise RuntimeError("Can't connect to ElasticJobParametersDB")

    self.indexName = "%s_%s" % (self.getIndexPrefix(), name.lower())
    # Verifying if the index is there, and if not create it
    res = self.existingIndex(self.indexName)
    if not res['OK'] or not res['Value']:
      result = self.createIndex(self.indexName, mapping, period=None)
      if not result['OK']:
        self.log.error(result['Message'])
        raise RuntimeError(result['Message'])
      self.log.always("Index created:", self.indexName)

    self.dslSearch = self._Search(self.indexName)

  def getJobParameters(self, jobID, paramList=None):
    """ Get Job Parameters defined for jobID.
      Returns a dictionary with the Job Parameters.
      If paramList is empty - all the parameters are returned.

    :param self: self reference
    :param int jobID: Job ID
    :param list paramList: list of parameters to be returned (also a string is treated)

    :return: dict with all Job Parameter values
    """

    if paramList:
      if isinstance(paramList, six.string_types):
        paramList = paramList.replace(' ', '').split(',')
    else:
       paramList = []

    self.log.debug('JobDB.getParameters: Getting Parameters for job %s' % jobID)

    resultDict = {}

    # the following should be equivalent to
    # {
    #   "query": {
    #     "bool": {
    #       "filter": {  # no scoring
    #         "term": {"JobID": jobID}  # term level query, does not pass through the analyzer
    #       }
    #     }
    #   }
    # }

    s = self.dslSearch.query("bool", filter=self._Q("term", JobID=jobID))

    res = s.execute()

    for hit in res:
      name = hit.Name
      if paramList and name not in paramList:
        continue
      resultDict[name] = hit.Value

    return S_OK({jobID: resultDict})

  def setJobParameter(self, jobID, key, value):
    """
    Inserts data into ElasticJobParametersDB index

    :param self: self reference
    :param int jobID: Job ID
    :param str key: parameter key
    :param str value: parameter value

    :returns: S_OK/S_ERROR as result of indexing
    """
    data = {"JobID": jobID,
            "Name": key,
            "Value": value}

    self.log.debug('Inserting data in %s:%s' % (self.indexName, data))

    result = self.index(self.indexName, body=data, docID=str(jobID) + key)
    if not result['OK']:
      self.log.error("ERROR: Couldn't insert data", result['Message'])
    return result

  def setJobParameters(self, jobID, parameters):
    """
    Inserts data into ElasticJobParametersDB index using bulk indexing

    :param self: self reference
    :param int jobID: Job ID
    :param list parameters: list of tuples (name, value) pairs

    :returns: S_OK/S_ERROR as result of indexing
    """
    self.log.debug(
        'Inserting parameters',
        "in %s: for job %s : %s" % (self.indexName, jobID, parameters))

    parametersListDict = [{"JobID": jobID,
                           "Name": parName,
                           "Value": parValue,
                           "_id": str(parName) + str(parValue)} for parName, parValue in parameters]

    result = self.bulk_index(self.indexName, data=parametersListDict, period=None, withTimeStamp=False)
    if not result['OK']:
      self.log.error("ERROR: Couldn't insert data", result['Message'])
    return result

  def deleteJobParameters(self, jobID, paramList=None):
    """ delete Job Parameters defined for jobID.
      Returns a dictionary with the Job Parameters.
      If paramList is empty - all the parameters for the job are removed

    :param self: self reference
    :param int jobID: Job ID
    :param list paramList: list of parameters to be returned (also a string is treated)

    :return: dict with all Job Parameter values
    """

    self.log.debug('JobDB.getParameters: Deleting Parameters %s for job %s' % (paramList, jobID))

    jobFilter = self._Q("term", JobID=jobID)

    if not paramList:
      s = self.dslSearch.query("bool", filter=jobFilter)
      s.delete()
      return S_OK()

    # the following should be equivalent to
    # {
    #   "query": {
    #     "bool": {
    #       "filter": [  # no scoring
    #         {"term": {"JobID": jobID}},  # term level query, does not pass through the analyzer
    #         {"term": {"Name": param}},  # term level query, does not pass through the analyzer
    #       ]
    #     }
    #   }
    # }

    if isinstance(paramList, six.string_types):
      paramList = paramList.replace(' ', '').split(',')

    for param in paramList:
      paramFilter = self._Q("term", Name=param)
      combinedFilter = jobFilter & paramFilter

      s = self.dslSearch.query("bool", filter=combinedFilter)
      s.delete()

    return S_OK()

  # TODO: Add query by value (e.g. query which values are in a certain pattern)
