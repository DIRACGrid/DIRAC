""" Module containing a front-end to the ElasticSearch-based ElasticJobParametersDB.
    This module interacts with one ES index: "ElasticJobParametersDB",
    which is a drop-in replacement for MySQL-based table JobDB.JobParameters.
    While JobDB.JobParameters in MySQL is defined as::

      ```
      CREATE TABLE `JobParameters` (
        `JobID` INT(11) UNSIGNED NOT NULL,
        `Name` VARCHAR(100) NOT NULL,
        `Value` BLOB NOT NULL,
        PRIMARY KEY (`JobID`,`Name`),
        FOREIGN KEY (`JobID`) REFERENCES `Jobs`(`JobID`)
      ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
      ```

    Here we define a mapping as::

      ```
      "JobID": {"type": "long"},
      "Name": {"type": "keyword"},
      "Value": {"type": "text"}
      ```

    which is an "equivalent" representation.

    The reason for switching to a ES-based JobParameters lies in the extended searching
    capabilities of ES (ES will analyze+index text fields, while MySQL won't do that on BLOB types).
    This results in higher traceability for DIRAC jobs.

    The following class methods are provided for public usage::
      - getJobParameters()
      - setJobParameter()
"""

from __future__ import absolute_import
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

    section = getDatabaseSection("WorkloadManagement/ElasticJobParametersDB")
    indexPrefix = gConfig.getValue("%s/IndexPrefix" % section, CSGlobals.getSetup()).lower()

    # Connecting to the ES cluster
    super(ElasticJobParametersDB, self).__init__(name, 'WorkloadManagement/ElasticJobParametersDB', indexPrefix)

    self.indexName = "%s_%s" % (self.getIndexPrefix(), name.lower())
    # Verifying if the index is there, and if not create it
    if not self.exists(self.indexName):
      result = self.createIndex(self.indexName, mapping, period=None)
      if not result['OK']:
        self.log.error(result['Message'])
      self.log.always("Index created:", self.indexName)

  def getJobParameters(self, jobID, paramList=None):
    """ Get Job Parameters defined for jobID.
      Returns a dictionary with the Job Parameters.
      If paramList is empty - all the parameters are returned.

    :param self: self reference
    :param int jobID: Job ID
    :param list paramList: list of parameters to be returned (also a string is treated)

    :return: dict with all Job Parameter values
    """

    self.log.debug('JobDB.getParameters: Getting Parameters for job %s' % jobID)

    resultDict = {}

    if paramList:
      if isinstance(paramList, six.string_types):
        paramList = paramList.replace(' ', '').split(',')

      query = {
          "query": {
              "bool": {
                  "must": [
                      {"match": {"JobID": jobID}},
                      {"match": {"Name": ','.join(paramList)}}
                  ]
              }
          },
          "_source": ["Name", "Value"]
      }

    else:
      query = {
          "query": {
              "match": {"JobID": jobID}
          },
          "_source": ["Name", "Value"]
      }

    result = self.query(self.indexName, query)

    if not result['OK']:
      return result

    sources = result['Value']['hits']['hits']

    for source in sources:

      name = source['_source']['Name']
      value = source['_source']['Value']

      try:
        resultDict[name] = value.tostring()
      except Exception:
        resultDict[name] = value

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

  def deleteJobParameters(self, jobID, paramList=None):
    """ delete Job Parameters defined for jobID.
      Returns a dictionary with the Job Parameters.
      If paramList is empty - all the parameters for the job are removed

    :param self: self reference
    :param int jobID: Job ID
    :param list paramList: list of parameters to be returned (also a string is treated)

    :return: dict with all Job Parameter values
    """

    self.log.debug('JobDB.getParameters: Deleting Parameters for job %s' % jobID)

    if paramList:
      if isinstance(paramList, six.string_types):
        paramList = paramList.replace(' ', '').split(',')

      query = {
          "query": {
              "bool": {
                  "must": [
                      {"match": {"JobID": jobID}},
                      {"match": {"Name": ','.join(paramList)}}
                  ]
              }
          }
      }

    else:
      query = {
          "query": {
              "match": {"JobID": jobID}
          }
      }

    result = self.deleteByQuery(self.indexName, query)

    return result


  # TODO: Add query by value (e.g. query which values are in a certain patter)
