########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/DB/JobDB.py,v 1.100 2008/09/14 21:29:24 atsareg Exp $
########################################################################

""" DIRAC JobDB class is a front-end to the main WMS database containing
    job definitions and status information. It is used in most of the WMS
    components

    The following methods are provided for public usage:

    getJobID()
    getJobAttribute()
    getJobAttributes()
    getAllJobAttributes()
    getDistinctJobAttributes()
    getAttributesForJobList()
    getJobParameter()
    getJobParameters()
    getAllJobParameters()
    getInputData()
    getSubjobs()
    getJobJDL()

    selectJobs()
    selectJobsWithStatus()

    setJobAttribute()
    setJobAttributes()
    setJobParameter()
    setJobParameters()
    setJobJDL()
    setJobStatus()
    setInputData()

    insertJobIntoDB()
    addJobToDB()
    removeJobFromDB()

    rescheduleJob()
    rescheduleJobs()

    getMask()
    setMask()
    allowSiteInMask()
    banSiteInMask()

    addQueue()
    selectQueue()
    addJobToQueue()
    deleteJobFromQueue()

    getCounters()
"""

__RCSID__ = "$Id: JobDB.py,v 1.100 2008/09/14 21:29:24 atsareg Exp $"

import re, os, sys, string, types
import time

from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd
from types                                     import *
from DIRAC                                     import gLogger, S_OK, S_ERROR, Time
from DIRAC.ConfigurationSystem.Client.Config   import gConfig
from DIRAC.Core.Base.DB                        import DB

DEBUG = 0

#############################################################################
class JobDB(DB):

  def __init__( self, maxQueueSize=10 ):
    """ Standard Constructor
    """

    DB.__init__(self,'JobDB','WorkloadManagement/JobDB',maxQueueSize)

    self.maxRescheduling = gConfig.getValue( self.cs_path+'/MaxRescheduling', 30)

    self.jobAttributeNames = []

    result = self.__getAttributeNames()

    if not result['OK']:
      error = 'Can not retrieve job Attributes'
      self.log.fatal( 'JobDB: %s' % error )
      sys.exit( error )
      return

    self.log.always("MaxReschedule:  "+`self.maxRescheduling`)
    self.log.always("==================================================")

    if DEBUG:
      result = self.dumpParameters()

  def dumpParameters(self):
    """  Dump the JobDB connection parameters to the stdout
    """

    print "=================================================="
    print "User:     ", self.dbUser
    print "Host:     ", self.dbHost
    print "Password  ", self.dbPass
    print "DBName    ", self.dbName
    print "MaxQueue  ", self.maxQueueSize
    print "=================================================="

    return S_OK()

  def __getAttributeNames(self):
    """ get Name of Job Attributes defined in DB
        set self.jobAttributeNames to the list of Names
        return S_OK()
        return S_ERROR upon error
    """

    res = self._query( 'DESCRIBE Jobs' )
    if not res['OK']:
      return res

    self.jobAttributeNames = []
    for Field, Type, Null, Key, Default, Extra in res['Value']:
      self.jobAttributeNames.append(Field)

    self.nJobAttributeNames = len(self.jobAttributeNames)

    return S_OK( )

  def __buildCondition(self, condDict, older=None, newer=None ):
    """ build SQL condition statement from provided condDict
        and other extra conditions
    """
    condition = ''
    conjunction = "WHERE"

    if condDict != None:
      for attrName, attrValue in condDict.items():
        if type(attrValue) == types.ListType:
          multiValue = ','.join(['"'+x.strip()+'"' for x in attrValue])
          condition = ' %s %s %s in (%s)' % ( condition,
                                             conjunction,
                                             str(attrName),
                                             multiValue  )
        else:
          condition = ' %s %s %s=\'%s\'' % ( condition,
                                             conjunction,
                                             str(attrName),
                                             str(attrValue)  )
        conjunction = "AND"

    if older:
      condition = ' %s %s LastUpdateTime < \'%s\'' % ( condition,
                                                 conjunction,
                                                 str(older) )
      conjunction = "AND"

    if newer:
      condition = ' %s %s LastUpdateTime >= \'%s\'' % ( condition,
                                                 conjunction,
                                                 str(newer) )

    return condition


#############################################################################
  def getJobID(self):
    """Get the next unique JobID and prepare the new job insertion
    """

    cmd = 'INSERT INTO Jobs (SubmissionTime) VALUES (UTC_TIMESTAMP())'
    err = 'JobDB.getJobID: Failed to retrieve a new Id.'

    res = self._getConnection()
    if not res['OK']:
      return S_ERROR( '0 %s\n%s' % (err, res['Message'] ) )

    connection = res['Value']
    res = self._update( cmd, connection )
    if not res['OK']:
      connection.close()
      return S_ERROR( '1 %s\n%s' % (err, res['Message'] ) )

    cmd = 'SELECT LAST_INSERT_ID()'
    res = self._query( cmd, connection )
    if not res['OK']:
      connection.close()
      return S_ERROR( '2 %s\n%s' % (err, res['Message'] ) )

    try:
      connection.close()
      jobID = int(res['Value'][0][0])
      self.log.info( 'JobDB: New JobID served "%s"' % jobID )
    except Exception, x:
      return S_ERROR( '3 %s\n%s' % (err, str(x) ) )

    return S_OK( jobID )

#############################################################################
  def getAttributesForJobList(self,jobIDList,attrList=[]):
    """ Get attributes for the jobs in the the jobIDList.
        Returns an S_OK structure with a dictionary of dictionaries as its Value:
        ValueDict[jobID][attribute_name] = attribute_value
    """

    if attrList:
      attrNames = string.join(map(lambda x: str(x),attrList ),',')
      attr_tmp_list = attrList
    else:
      attrNames = string.join(map(lambda x: str(x),self.jobAttributeNames),',')
      attr_tmp_list = self.jobAttributeNames
    jobList = string.join(map(lambda x: str(x),jobIDList),',')

    cmd = 'SELECT JobID,%s FROM Jobs WHERE JobID in ( %s )' % ( attrNames, jobList )
    res = self._query( cmd )
    if not res['OK']:
      return res
    try:
      retDict = {}
      for retValues in res['Value']:
        jobID = retValues[0]
        jobDict = {}
        jobDict[ 'JobID' ] = jobID
        attrValues = retValues[1:]
        for i in range(len(attr_tmp_list)):
          try:
            jobDict[attr_tmp_list[i]] = attrValues[i].tostring()
          except:
            jobDict[attr_tmp_list[i]] = str(attrValues[i])
        retDict[int(jobID)] = jobDict
      return S_OK( retDict )
    except Exception,x:
      return S_ERROR( 'JobDB.getAttributesForJobList: Failed\n%s'  % str(x) )


#############################################################################
  def getDistinctJobAttributes(self,attribute, condDict = {}, older = None, newer=None):
    """ Get distinct values of the job attribute under specified conditions
    """
    cmd = 'SELECT  DISTINCT(%s) FROM Jobs ORDER BY %s' % (attribute,attribute)

    cond = self.__buildCondition( condDict, older=older, newer=newer )

    result = self._query( cmd + cond )
    if not result['OK']:
      return result

    attr_list = [ x[0] for x in result['Value'] ]
    return S_OK(attr_list)

#############################################################################
  def getJobParameters(self, jobID, paramList=[]):
    """ Get Job Parameters defined for jobID.
        Returns a dictionary with the Job Parameters.
        If parameterList is empty - all the parameters are returned.
    """

    self.log.debug( 'JobDB.getParameters: Getting Parameters for job %d' % int(jobID) )

    resultDict = {}
    if paramList:
      paramNames = string.join(map(lambda x: '"'+str(x)+'"',paramList ),',')
      cmd = "SELECT Name, Value from JobParameters WHERE JobID=%d and Name in (%s)" % (int(jobID),paramNames)
      result = self._query(cmd)
      if result['OK']:
        if result['Value']:
          for name,value in result['Value']:
            try:
              resultDict[name] = value.tostring()
            except:
              resultDict[name] = value

        return S_OK(resultDict)
      else:
        return S_ERROR('JobDB.getJobParameters: failed to retrieve parameters')

    else:
      result = self._getFields( 'JobParameters',['Name', 'Value'],['JobID'], [jobID])
      if not result['OK']:
        return result
      else:
        for name,value in result['Value']:
          try:
            resultDict[name] = value.tostring()
          except:
            resultDict[name] = value

        return S_OK(resultDict)

#############################################################################
  def getJobAttributes(self,jobID,attrList=[]):
    """ Get all Job Attributes for a given jobID.
        Return a dictionary with all Job Attributes,
        return an empty dictionary if matching job found
    """

    if attrList:
      attrNames = string.join(map(lambda x: str(x),attrList ),',')
    else:
      attrNames = string.join(map(lambda x: str(x),self.jobAttributeNames),',')
    self.log.debug( 'JobDB.getAllJobAttributes: Getting Attributes for job = "%s".' %jobID )

    cmd = 'SELECT %s FROM Jobs WHERE JobID=%d' % (attrNames,int(jobID))
    res = self._query( cmd )
    if not res['OK']:
      return res

    if len(res['Value']) == 0:
      return S_OK ( {} )

    values = res['Value'][0]

    attributes = {}
    if attrList:
      for i in range(len(attrList)):
        attributes[attrList[i]] = str(values[i])
    else:
      for i in range(len(self.jobAttributeNames)):
        attributes[self.jobAttributeNames[i]] = str(values[i])

    return S_OK( attributes )

#############################################################################
  def getJobInfo( self, jobID, parameters=[] ):
    """ Get parameters for job specified by jobID. Parameters can be
        either job attributes ( fields in the Jobs table ) or those
        stored in the JobParameters table.
        The return value is a dictionary of the structure:
        Dict[Name] = Value
    """

    resultDict = {}
    # Parameters are not specified, get them all - parameters + attributes
    if not parameters:
      result = self.getJobAttributes(jobID)
      if result['OK']:
        resultDict = result['value']
      else:
       return S_ERROR('JobDB.getJobAttributes: can not retrieve job attributes')
      result = self.getJobParameters(jobID)
      if result['OK']:
        resultDict.update(result['value'])
      else:
       return S_ERROR('JobDB.getJobParameters: can not retrieve job parameters')
      return S_OK(resultDict)

    paramList = []
    attrList = []
    for p in parameters:
      if p in self.jobAttributeNames:
        attrList.append(p)
      else:
        paramList.append(p)

    # Get Job Attributes first
    if attrList:
      result = self.getJobAttributes(jobID,attrList)
      if not result['OK']:
        return result
      if len(res['Value']) > 0:
        resultDict = result['Value']
      else:
        return S_ERROR('Job '+str(jobID)+' not found')

    # Get Job Parameters
    if paramList:
      result = self.getJobParameters(jobID,paramList)
      if not result['OK']:
        return result
      if len(res['Value']) > 0:
        resultDict.update(result['Value'])

    return S_OK(resultDict)

#############################################################################
  def getJobAttribute(self,jobID,attribute):
    """ Get the given attribute of a job specified by its jobID
    """

    result = self.getJobAttributes(jobID,[attribute])
    if result['OK']:
      value = result['Value'][attribute]
      return S_OK(value)
    else:
      return result

 #############################################################################
  def getJobParameter(self,jobID,parameter):
    """ Get the given parameter of a job specified by its jobID
    """

    result = self.getJobParameters(jobID,[parameter])
    if result['OK']:
      if result['Value']:
        value = result['Value'][parameter]
      else:
        value = None
      return S_OK(value)
    else:
      return result

 #############################################################################
  def getJobOptParameter(self,jobID,parameter):
    """ Get optimizer parameters for the given job.
    """

    result = self._getFields( 'OptimizerParameters',['Value'],['JobID','Name'], [jobID,parameter])
    if result['OK']:
      if result['Value']:
        return S_OK(result['Value'][0][0])
      else:
        return S_ERROR('Parameter not found')
    else:
      return S_ERROR('Failed to access database')

 #############################################################################
  def getJobOptParameters(self,jobID,paramList=[]):
    """ Get optimizer parameters for the given job. If the list of parameter names is
        empty, get all the parameters then
    """

    resultDict = {}

    if paramList:
      paramNames = ','.join( ['"'+str(x)+'"' for x in paramList ] )
      cmd = "SELECT Name, Value from OptimizerParameters WHERE JobID=%d and Name in (%s)" % (int(jobID),paramNames)
    else:
      cmd = "SELECT Name, Value from OptimizerParameters WHERE JobID=%d" % jobID

    result = self._query(cmd)
    if result['OK']:
      if result['Value']:
        for name,value in result['Value']:
          try:
            resultDict[name] = value.tostring()
          except:
            resultDict[name] = value

      return S_OK(resultDict)
    else:
      return S_ERROR('JobDB.getJobOptParameters: failed to retrieve parameters')

#############################################################################
  def getInputData (self, jobID):
    """Get input data for the given job
    """
    cmd = 'SELECT LFN FROM InputData WHERE JobID=\'%s\'' %jobID
    res = self._query(cmd)
    if not res['OK']:
      return res

    return S_OK( map( self._to_value, res['Value'] ) )

#############################################################################
  def setInputData (self, jobID, inputData):
    """Inserts input data for the given job
    """
    cmd = 'DELETE FROM InputData WHERE JobID=\'%s\'' % (jobID)
    result = self._update( cmd )
    if not result['OK']:
      result = S_ERROR('JobDB.setInputData: operation failed.')

    for lfn in inputData:
      cmd = 'INSERT INTO InputData (JobID,LFN) VALUES (\'%s\', \'%s\' )' % ( jobID, lfn.strip() )
      res = self._update( cmd )
      if not res['OK']:
        return res

    return S_OK('Files added')

#############################################################################
  def setOptimizerChain(self,jobID,optimizerList):
    """ Set the optimizer chain for the given job. The 'TaskQueue'
        optimizer should be the last one in the chain, it is added
        if not present in the optimizerList
    """

    optString = string.join(optimizerList,',')
    result = self.setJobOptParameter(jobID,'OptimizerChain',optString)
    return result

 #############################################################################
  def setNextOptimizer(self,jobID,currentOptimizer):
    """ Set the job status to be processed by the next optimizer in the
        chain
    """

    result = self.getJobOptParameter(jobID,'OptimizerChain')
    if not result['OK']:
      return result

    optListString = result['Value']
    optList = optListString.split(',')
    try:
      sindex = None
      for i in xrange(len(optList)):
        if optList[i] == currentOptimizer:
          sindex = i
      if sindex >= 0:
        if sindex < len(optList)-1:
          nextOptimizer = optList[sindex+1]
        else:
          return S_ERROR('Unexpected end of the Optimizer Chain')
      else:
        return S_ERROR('Could not find '+currentOptimizer+' in chain')
    except ValueError, x:
      return S_ERROR('The '+currentOptimizer+' not found in the chain')

    return self.setJobStatus(jobID,status="Checking",minor=nextOptimizer)

############################################################################
  def countJobs(self, condDict, older=None, newer=None):
    """ Get the number of jobs matching conditions specified by condDict and time limits
    """
    self.log.debug ( 'JobDB.countJobs: counting Jobs' )
    cond = self.__buildCondition(condDict, older, newer)
    cmd = ' SELECT count(JobID) from Jobs '
    ret = self._query( cmd + cond )
    if ret['OK']:
      return S_OK(ret['Value'][0][0])
    return ret

#############################################################################
  def selectJobs(self, condDict, older=None, newer=None, orderAttribute=None, limit=None ):
    """ Select jobs matching the following conditions:
        - condDict dictionary of required Key = Value pairs;
        - with the last update date older and/or newer than given dates;

        The result is ordered by JobID if requested, the result is limited to a given
        number of jobs if requested.
    """

    self.log.debug( 'JobDB.selectJobs: retrieving jobs.' )

    condition = self.__buildCondition(condDict, older, newer)

    if orderAttribute:
      orderType = None
      orderField = orderAttribute
      if orderAttribute.find(':') != -1:
        orderType = orderAttribute.split(':')[1].upper()
        orderField = orderAttribute.split(':')[0]
      condition = condition + ' ORDER BY ' + orderField
      if orderType:
        condition = condition + ' ' + orderType

    if limit:
      condition = condition + ' LIMIT ' + str(limit)

    cmd = 'SELECT JobID from Jobs ' + condition
    res = self._query( cmd )
    if not res['OK']:
      return res

    if not len(res['Value']):
      return S_OK([])
    return S_OK( map( self._to_value, res['Value'] ) )

#############################################################################
  def selectJobWithStatus(self,status):
    """ Get the list of jobs with a given Major Status
    """

    return self.selectJobs({'Status':status})

#############################################################################
  def setJobAttribute(self, jobID, attrName, attrValue, update=False, datetime=None ):
    """ Set an attribute value for job specified by jobID.
        The LastUpdate time stamp is refreshed if explicitely requested
    """

    if update:
      cmd = 'UPDATE Jobs SET %s=\'%s\',LastUpdateTime=UTC_TIMESTAMP() WHERE JobID=\'%s\'' % ( attrName, attrValue, jobID )
    else:
      cmd = 'UPDATE Jobs SET %s=\'%s\' WHERE JobID=\'%s\'' % ( attrName, attrValue, jobID )

    if datetime:
      cmd += ' AND LastUpdateTime < %s' % datetime

    res = self._update( cmd )
    if res['OK']:
      return res
    else:
      return S_ERROR( 'JobDB.setAttribute: failed to set attribute' )

#############################################################################
  def setJobAttributes(self, jobID, attrNames, attrValues, update=False, datetime=None ):
    """ Set an attribute value for job specified by jobID.
        The LastUpdate time stamp is refreshed if explicitely requested
    """

    if len(attrNames) != len(attrValues):
      return S_ERROR( 'JobDB.setAttributes: incompatible Argument length' )

    attr = []
    for i in range(len(attrNames)):
      attr.append( '%s=\'%s\'' % (attrNames[i],attrValues[i]))
    if update:
      attr.append( "LastUpdateTime=UTC_TIMESTAMP()" )
    if len(attr) == 0:
      return S_ERROR( 'JobDB.setAttributes: Nothing to do' )

    cmd = 'UPDATE Jobs SET %s WHERE JobID=\'%s\'' % ( ', '.join(attr), jobID )

    if datetime:
      cmd += ' AND LastUpdateTime < %s' % datetime

    res = self._update( cmd )
    if res['OK']:
      return res
    else:
      return S_ERROR( 'JobDB.setAttributes: failed to set attribute' )

#############################################################################
  def setJobStatus(self,jobID,status='',minor='',application='',appCounter=None):
    """ Set status of the job specified by its jobID
    """

    # Do not update the LastUpdate time stamp if setting the Stalled status
    update_flag = True
    if status == "Stalled":
      update_flag = False

    attrNames = []
    attrValues = []
    if status:
      attrNames.append('Status')
      attrValues.append(status)
    if minor:
      attrNames.append('MinorStatus')
      attrValues.append(minor)
    if application:
      attrNames.append('ApplicationStatus')
      attrValues.append(application)
    if appCounter:
      attrNames.append('ApplicationNumStatus')
      attrValues.append(appCounter)

    result = self.setJobAttributes(jobID,attrNames,attrValues,update=True)
    if not result['OK']:
      return result

    return S_OK()

#############################################################################
  def setJobParameter(self,jobID,key,value):
    """ Set a parameter specified by name,value pair for the job JobID
    """

    cmd = 'DELETE FROM JobParameters WHERE JobID=\'%s\' AND Name=\'%s\'' % ( jobID, key )
    if not self._update( cmd )['OK']:
      result = S_ERROR('JobDB.setJobParameter: operation failed.')

    result = self._insert('JobParameters',['JobID','Name','Value'],[jobID, key, value])
    if not result['OK']:
      result = S_ERROR('JobDB.setJobParameter: operation failed.')

    return result

#############################################################################
  def setJobParameters(self,jobID,parameters):
    """ Set parameters specified by a list of name/value pairs for the job JobID
    """

    if not parameters:
      return S_OK()

    deleteCondList = []
    insertValueList = []
    for name,value in parameters:
      if type(value) in types.StringTypes:
        ret = self._escapeString(value)
        if not ret['OK']:
          return ret
        value = ret['Value']
      deleteCondList.append( '(JobID=\'%s\' AND Name=\'%s\')' % (jobID, name))
      insertValueList.append( '(\'%s\',\'%s\',%s)' % (jobID, name, value))

    cmd = 'DELETE FROM JobParameters WHERE %s ' % ' OR '.join( deleteCondList )
    if not self._update( cmd )['OK']:
      result = S_ERROR('JobDB.setJobParameters: operation failed.')

    cmd = 'INSERT INTO JobParameters (JobID,Name,Value) VALUES %s' % ', '.join(insertValueList)

    result = self._update( cmd )
    if not result['OK']:
      return S_ERROR('JobDB.setJobParameters: operation failed.')

    return result

 #############################################################################
  def setJobOptParameter(self,jobID,name,value):
    """ Set an optimzer parameter specified by name,value pair for the job JobID
    """

    cmd = 'DELETE FROM OptimizerParameters WHERE JobID=\'%s\' AND Name=\'%s\'' % ( jobID, name )
    if not self._update( cmd )['OK']:
      result = S_ERROR('JobDB.setJobOptParameter: operation failed.')

    result = self._insert('OptimizerParameters',['JobID','Name','Value'],[jobID, name, value])
    if not result['OK']:
      return S_ERROR('JobDB.setJobOptParameter: operation failed.')

    return S_OK()

#############################################################################
  def removeJobOptParameter(self,jobID,name):
    """ Remove the specified optimizer parameter for jobID
    """

    cmd = 'DELETE FROM OptimizerParameters WHERE JobID=\'%s\' AND Name=\'%s\'' % ( jobID, name )
    if not self._update( cmd )['OK']:
      return S_ERROR('JobDB.removeJobOptParameter: operation failed.')
    else:
      return S_OK()

#############################################################################
  def setAtticJobParameter(self,jobID,key,value,rescheduleCounter):
    """ Set attic parameter for job specified by its jobID when job rescheduling
        for later debugging
    """

    cmd = 'INSERT INTO AtticJobParameters VALUES(%d,%d,\'%s\',\'%s\')' % \
         (int(jobID),rescheduleCounter,key,value)
    result = self._update( cmd )
    if not result['OK']:
      result = S_ERROR('JobDB.setAtticJobParameter: operation failed.')

    return result

#############################################################################
  def __setInitialSite( self, classadJob, jobID):
    """ Set initial site assignement for the job
    """

    #
    #  Site should be extracted from the corresponding parameter
    #
    site='ANY'
    requirements = ''
    if classadJob.lookupAttribute("Requirements"):
       requirements  = classadJob.get_expression("Requirements")
    if requirements:
      if string.find( string.upper(requirements),string.upper("Other.Site"))>=0:
        requirements = string.split(requirements," ")
        i = 0
        for requirement in requirements:
          if string.upper(requirement) == string.upper('Other.Site'):
            if len(requirements) >= i+3:
              site = string.replace(requirements[i+2],'"','')
            else:
              site='ANY'
          i += 1
    result = self.setJobAttribute( jobID, 'Site', site )
    if not result['OK']:
      return result

    return S_OK()

#############################################################################
  def __setInitialJobParameters( self, classadJob, jobID):
    """ Set initial job parameters as was defined in the Classad
    """

    # Extract initital job parameters
    parameters= {}
    if classadJob.lookupAttribute("Parameters"):
      parameters= classadJob.getDictionaryFromSubJDL("Parameters")
    res = self.setJobParameters(jobID, parameters.items())

    if not res['OK']:
      return res

    return S_OK()

#############################################################################
  def setJobJDL(self, jobID, JDL=None, originalJDL = None):
    """ Insert JDL's for job specified by jobID
    """

    req = "SELECT OriginalJDL FROM JobJDLs WHERE JobID=%s" % jobID
    result = self._query(req)
    updateFlag = False
    if result['OK']:
      if len(result['Value']) > 0:
        updateFlag = True

    if JDL:

      if updateFlag:
        cmd = "UPDATE JobJDLs Set JDL='%s' WHERE JobID=%s" % (JDL,jobID)
      else:
        cmd = "INSERT INTO JobJDLs (JobID,JDL) VALUES (%s,'%s')" % (jobID,JDL)
      result = self._update(cmd)
      if not result['OK']:
        return result
    if originalJDL:
      if updateFlag:
        cmd = "UPDATE JobJDLs Set OriginalJDL='%s' WHERE JobID=%s" % (originalJDL,jobID)
      else:
        cmd = "INSERT INTO JobJDLs (JobID,OriginalJDL) VALUES (%s,'%s')" % (jobID,originalJDL)

      result = self._update(cmd)

    return result

#############################################################################
  def __insertNewJDL( self, JDL ):
    """Insert a new JDL in the system, this produces a new JobID
    """
    res = self._getConnection()
    if not res['OK']:
      return False
    connection = res['Value']
    res = self._insert( 'JobJDLs' , ['OriginalJDL'],[JDL], connection)

    cmd = 'SELECT LAST_INSERT_ID()'
    res = self._query( cmd, connection )
    if not res['OK']:
      connection.close()
      self.log.error( 'Can not retrieve LAST_INSERT_ID', res['Message'] )
      return False

    try:
      connection.close()
      jobID = int(res['Value'][0][0])
      self.log.info( 'JobDB: New JobID served "%s"' % jobID )
    except Exception, x:
      self.log.exception( 'Exception retrieving LAST_INSERT_ID' )
      return False

    return jobID


#############################################################################
  def getJobJDL(self,jobID,original=False,status=''):
    """ Get JDL for job specified by its jobID. By default the current job JDL
        is returned. If 'original' argument is True, original JDL is returned
    """

    if original:
      cmd = "SELECT OriginalJDL FROM JobJDLs WHERE JobID=%d" % int(jobID)
    else:
      cmd = "SELECT JDL FROM JobJDLs WHERE JobID=%d" % int(jobID)

    if status:
      cmd = cmd + " AND Status='%s'" % status

    result = self._query(cmd)
    if result['OK']:
      jdl = result['Value']
      if not jdl:
        return S_OK(jdl)
      else:
        return S_OK(result['Value'][0][0])
    else:
      return result

#############################################################################
  def insertJobIntoDB(self, jobID, JDL):
    """ Insert the initial job JDL into the Job database
    """

    result = self.setJobJDL(jobID,originalJDL=JDL)
    if not result['OK']:
      return result

    return self.setJobStatus(jobID,status='Received',minor='Initial insertion')

#############################################################################
  def insertNewJobIntoDB(self, JDL, owner, ownerDN, ownerGroup, diracSetup ):
    """ Insert the initial JDL into the Job database,
        Do initial JDL crosscheck,
        Set Initial job Attributes and Status
    """

    jobAttrNames  = []
    jobAttrValues = []

    # 1.- insert original JDL on DB and get new JobID
    jobID = self.__insertNewJDL( JDL )
    if not jobID:
      return S_ERROR( 'Can not insert JDL in to DB' )

    jobAttrNames.append('JobID')
    jobAttrValues.append(jobID)

    jobAttrNames.append('LastUpdateTime')
    jobAttrValues.append(Time.toString())

    jobAttrNames.append('SubmissionTime')
    jobAttrValues.append(Time.toString())

    jobAttrNames.append('Owner')
    jobAttrValues.append(owner)

    jobAttrNames.append('OwnerDN')
    jobAttrValues.append(ownerDN)

    jobAttrNames.append('OwnerGroup')
    jobAttrValues.append(ownerGroup)

    jobAttrNames.append('DIRACSetup')
    jobAttrValues.append(diracSetup)

    # 2.- Check JDL and Prepare DIRAC JDL
    classAdJob = ClassAd( '[%s]' % JDL )
    classAdReq = ClassAd( '[]' )
    retVal = S_OK(jobID)
    retVal['JobID'] = jobID
    if not classAdJob.isOK():
      jobAttrNames.append('Status')
      jobAttrValues.append('Failed')

      jobAttrNames.append('MinorStatus')
      jobAttrValues.append('Error in JDL syntax')

      result = self._insert( 'Jobs', jobAttrNames, jobAttrValues )
      if not result['OK']:
        return result

      retVal['Status'] = 'Failed'
      retVal['MinorStatus'] = 'Error in JDL syntax'
      return retVal

    classAdJob.insertAttributeInt( 'JobID', jobID )
    result = self.__checkAndPrepareJob( classAdJob, classAdReq, owner, ownerDN, ownerGroup, diracSetup )
    if not result['OK']:
      jobAttrNames.append('Status')
      jobAttrValues.append('Failed')

      jobAttrNames.append('MinorStatus')
      jobAttrValues.append(result['Message'])

      resultInsert = self._insert( 'Jobs', jobAttrNames, jobAttrValues )
      if not resultInsert['OK']:
        return resultInsert

      retVal['Status'] = 'Failed'
      retVal['MinorStatus'] = result['Message']
      return retVal

    priority      = classAdJob.getAttributeInt( 'Priority' )
    jobAttrNames.append( 'UserPriority' )
    jobAttrValues.append( priority )

    for jdlName in 'JobName', 'JobType', 'JobGroup', 'Site':
      # Defaults are set by the DB.
      jdlValue = classAdJob.getAttributeString( jdlName )
      if jdlValue:
        jobAttrNames.append( jdlName )
        jobAttrValues.append( jdlValue )

    jobAttrNames.append('VerifiedFlag')
    jobAttrValues.append('True')

    jobAttrNames.append('Status')
    jobAttrValues.append('Received')

    jobAttrNames.append('MinorStatus')
    jobAttrValues.append('Job accepted')

    reqJDL = classAdReq.asJDL()
    classAdJob.insertAttributeInt( 'JobRequirements', reqJDL )

    jobJDL = classAdJob.asJDL()

    result = self.setJobJDL( jobID, jobJDL )
    if not result['OK']:
      return result

    inputData = []
    if classAdJob.lookupAttribute('InputData'):
      inputData = classAdJob.getListFromExpression('InputData')
    values = []
    for lfn in inputData:
      values.append( '(%s, \'%s\' )' % ( jobID, lfn.strip() ) )

    if values:
      cmd = 'INSERT INTO InputData (JobID,LFN) VALUES %s' % ', '.join( values )
      result = self._update( cmd )
      if not result['OK']:
        return result

    result = self.__setInitialJobParameters(classAdJob,jobID)
    if not result['OK']:
      return result

    result = self._insert( 'Jobs', jobAttrNames, jobAttrValues )
    if not result['OK']:
      return result

    retVal['Status'] = 'Received'
    retVal['MinorStatus'] = 'Job accepted'

    return retVal

  def __checkAndPrepareJob(self, classAdJob, classAdReq, owner, ownerDN, ownerGroup, diracSetup ):
    """
      Check Consistence of Submitted JDL and set some defaults
      Prepare subJDL with Job Requirements
    """
    error = ''

    jdlDiracSetup = classAdJob.getAttributeString( 'DIRACSetup' )
    jdlOwner      = classAdJob.getAttributeString( 'Owner' )
    jdlOwnerDN    = classAdJob.getAttributeString( 'OwnerDN' )
    jdlOwnerGroup = classAdJob.getAttributeString( 'OwnerGroup' )

    # The below is commented out since this is always overwritten by the submitter IDs
    #if jdlDiracSetup and jdlDiracSetup != diracSetup:
    #  error = 'Wrong DIRAC Setup in JDL'
    #if jdlOwner and jdlOwner != owner:
    #  error = 'Wrong Owner in JDL'
    #elif jdlOwnerDN and jdlOwnerDN != ownerDN:
    #  error = 'Wrong Owner DN in JDL'
    #elif jdlOwnerGroup and jdlOwnerGroup != ownerGroup:
    #  error = 'Wrong Owner Group in JDL'

    classAdJob.insertAttributeString( 'Owner',      owner )
    classAdJob.insertAttributeString( 'OwnerDN',    ownerDN )
    classAdJob.insertAttributeString( 'OwnerGroup', ownerGroup )

    classAdReq.insertAttributeString( 'Setup',      diracSetup )
    classAdReq.insertAttributeString( 'OwnerDN',    ownerDN )
    classAdReq.insertAttributeString( 'OwnerGroup', ownerGroup )

    voPolicyDict = gConfig.getOptionsDict('/DIRAC/VOPolicy')
    if voPolicyDict['OK']:
      voPolicy = voPolicyDict['Value']
      for param,val in voPolicy.items():
        if not classAdJob.lookupAttribute(param):
          classAdJob.insertAttributeString(param,val)

    if not classAdJob.lookupAttribute("Requirements"):
      # No requirements given in the job
      classAdJob.insertAttributeBool("Requirements", True)

    priority      = classAdJob.getAttributeInt( 'Priority' )
    systemConfig  = classAdJob.getAttributeString( 'SystemConfig' )
    pilotType     = classAdJob.getAttributeString( 'PilotType' )
    cpuTime       = classAdJob.getAttributeInt( 'MaxCPUTime' )

    classAdReq.insertAttributeInt( 'UserPriority', priority )

    classAdReq.insertAttributeInt(    'CPUTime',    cpuTime )

    if systemConfig and systemConfig.lower() != 'any':
      # Get the LHCb Platforms that are compatible with the requested systemConfig
      result = gConfig.getOptionsDict('/Resources/Computing/OSCompatibility')
      if result['OK'] and result['Value']:
        platforms = result['Value']
        lhcbPlatforms = []
        for platform in platforms:
          if systemConfig in [ x.strip() for x in platforms[platform].split(',') ]:
            lhcbPlatforms.append( platform )
        if lhcbPlatforms:
          classAdReq.insertAttributeVectorString( 'LHCbPlatforms', lhcbPlatforms )
        else:
          error = 'No compatible Platform found for %s' % systemConfig

    if pilotType:
      classAdReq.insertAttributeString( 'PilotType', pilotType )

    if error:
      return S_ERROR(error)

    return S_OK()


#############################################################################
  def addJobToDB (self, jobID, JDL=None, ownerDN='Unknown', ownerGroup = "Unknown"):
    """Insert new job to Job DB and extract job characteristics for specific
       lookups.
    """

    jdl = JDL
    if not jdl:
      result = self.getJobJDL(jobID,original=True)
      if result['OK']:
        jdl = result['Value']

    classadJob = ClassAd(jdl)

    if not classadJob.isOK():
      self.log.error( "JobDB.addJobToDB: Error in JDL syntax" )
      result = self.setJobStatus(jobID,minor='Verification Failed')
      result = self.setJobParameter(jobID,'VerificationError','Error in JDL syntax')
      return S_ERROR( "JobDB.addJobToDB: Error in JDL syntax" )

    if classadJob.lookupAttribute("InputData"):
      inputData = classadJob.getListFromExpression("InputData")
    else:
      inputData = []

    if classadJob.lookupAttribute("Owner"):
      owner = classadJob.get_expression("Owner").replace('"','')
    else:
      owner = "Unknown"

    if classadJob.lookupAttribute("JobGroup"):
      jobGroup = classadJob.get_expression("JobGroup").replace('"','')
    else:
      jobGroup = "NoGroup"

    if classadJob.lookupAttribute("JobName"):
      jobName = classadJob.get_expression("JobName").replace('"','')
    else:
      jobName = "Unknown"

    if classadJob.lookupAttribute("DIRACSetup"):
      diracSetup = classadJob.get_expression("DIRACSetup").replace('"','')
    else:
      result = gConfig.getOption('/LocalSite/DIRACSetup')
      if result['OK']:
        diracSetup = result['Value']
      else:
        diracSetup = "Unknown"

    if not classadJob.lookupAttribute("Requirements"):
      # No requirements given in the job
      classadJob.insertAttributeBool("Requirements", True)

    if classadJob.lookupAttribute("JobType"):
      jobType = classadJob.get_expression("JobType").replace('"','')
    else:
      jobType = "normal"

    if classadJob.lookupAttribute("Priority"):
      priority = classadJob.get_expression("Priority")
    else:
      priority = 0

    cmd = 'UPDATE Jobs SET JobName=\'%s\', JobType=\'%s\', DIRACSetup=\'%s\',' \
          'Owner=\'%s\', OwnerDN=\'%s\', OwnerGroup=\'%s\', ' \
          'JobGroup=\'%s\', UserPriority=\'%s\' WHERE JobID=\'%s\' ' \
          % ( jobName, jobType, diracSetup,
              owner, ownerDN, ownerGroup, jobGroup, priority, jobID )

    res = self._update( cmd )
    if not res['OK']:
      return res

    for lfn in inputData:
      cmd = 'INSERT INTO InputData (JobID,LFN) VALUES (\'%s\', \'%s\' )' % ( jobID, lfn.strip() )
      res = self._update( cmd )
      if not res['OK']:
        return res

    result = self.__setInitialJobParameters(classadJob,jobID)
    if not result['OK']:
      return result

    result = self.__setInitialSite(classadJob,jobID)
    if not result['OK']:
      return result

    result = self.setJobStatus(jobID,status='Received',minor='Job accepted')
    result = self.setJobAttribute(jobID,'VerifiedFlag','True')

    result = S_OK()
    result['InputData']    = classadJob.lookupAttribute("InputData")
    result['CEUniqueId']   = classadJob.lookupAttribute("CEUniqueId")
    result['Site']         = classadJob.lookupAttribute("Site")
    result['Requirements'] = classadJob.get_expression("Requirements")
    result['JobID']        = jobID

    return result

#############################################################################
  def removeJobFromDB(self, jobID):
    """Remove job from DB

       Remove job from the Job DB and clean up all the job related data
       in various tables
    """

    # If this is a master job delete the children first
    failedSubjobList = []
    result = self.getJobAttribute(jobID,'JobSplitType')
    if result['OK']:
      if result['Value'] == "Master":
        result = self.getSubjobs(jobID)
        if result['OK']:
          subjobs = result['Value']
          if subjobs:
            for job in subjobs:
              result = self.removeJobFromDB(job)
              if not result['OK']:
                failedSubjobList.append(job)
                self.log.error("Failed to delete job "+str(job)+" from JobDB")

    failedTablesList = []
    for table in ( 'Jobs',
                   'JobJDLs',
                   'InputData',
                   'JobParameters',
                   'AtticJobParameters',
                   'TaskQueue'):

      cmd = 'DELETE FROM %s WHERE JobID=\'%s\'' % ( table, jobID )
      result = self._update( cmd )
      if not result['OK']:
        failedTablesList.append(table)

    result = S_OK()
    if failedSubjobList:
      result = S_ERROR('Errors while job removal')
      result['FailedSubjobs'] = failedSubjobList
    if failedTablesList:
      result = S_ERROR('Errors while job removal')
      result['FailedTables'] = failedTablesList

    return result

#################################################################
  def getSubjobs(self,jobID):
    """ Get subjobs of the given job
    """

    cmd = "SELECT SubJobID FROM SubJobs WHERE JobID=%d" % ind(jobID)
    result = self._query(cmd)
    subjobs = []
    if result['OK']:
      subjobs = [ int(x[0]) for x in result['Value']]
      return S_OK(subjobs)
    else:
      return result

#################################################################
  def rescheduleJobs(self, jobIDs ):
    """ Reschedule all the jobs in the given list
    """

    result = S_OK()

    failedJobs = []
    for jobID in jobIDs:
      result = self.rescheduleJob(jobID)
      if not result['OK']:
        failedJobs.append(jobID)

    if failedJobs:
      result = S_ERROR('JobDB.rescheduleJobs: Not all the jobs were rescheduled')
      result['FailedJobs'] = failedJobs

    return result

#############################################################################
  def rescheduleJob (self, jobID):
    """ Reschedule the given job to run again from scratch. Retain the already
        defined parameters in the parameter Attic
    """
    # Check Verified Flag
    result = self.getJobAttributes( jobID, ['Status','MinorStatus','VerifiedFlag','RescheduleCounter',
                                     'Owner','OwnerDN','OwnerGroup','DIRACSetup'] )
    if result['OK']:
      resultDict = result['Value']
    else:
     return S_ERROR('JobDB.getJobAttributes: can not retrieve job attributes')

    if not 'VerifiedFlag' in resultDict:
      return S_ERROR('Job '+str(jobID)+' not found in the system')

    if not resultDict['VerifiedFlag']:
      return S_ERROR('Job %s not Verified: Status = %s, MinorStatus = %s' % (
                                                                             jobID,
                                                                             resultDict['Status'],
                                                                             resultDict['MinorStatus'] ) )


    # Check the Reschedule counter first
    rescheduleCounter = int(resultDict['RescheduleCounter']) + 1

    # Exit if the limit of the reschedulings is reached
    if rescheduleCounter > self.maxRescheduling:
      self.log.error('Maximum number of reschedulings is reached for job %s' % jobID)
      res = self.setJobStatus(jobID, status='Failed', minor='Maximum of reschedulings reached')
      return S_ERROR('Maximum number of reschedulings is reached: %s' % self.maxRescheduling)

    jobAttrNames  = []
    jobAttrValues = []

    jobAttrNames.append(  'RescheduleCounter' )
    jobAttrValues.append( rescheduleCounter )

    # Save the job parameters for later debugging
    result = self.getJobParameters(jobID)
    if result['OK']:
      parDict = result['Value']
      for key,value in parDict.items():
        result = self.setAtticJobParameter(jobID,key,value,rescheduleCounter-1)
        if not result['OK']:
          break

    cmd = 'DELETE FROM JobParameters WHERE JobID=\'%s\'' %jobID
    res = self._update( cmd )
    if not res['OK']:
      return res

    # the Jobreceiver needs to know if there is InputData ??? to decide which optimizer to call
    # proposal: - use the getInputData method
    res = self.getJobJDL( jobID,original=True)
    if not res['OK']:
      return res

    jdl = res['Value']

    # Restore initital job parameters
    classAdJob = ClassAd('['+jdl+']')
    classAdReq = ClassAd('[]')
    retVal = S_OK(jobID)
    retVal['JobID'] = jobID

    classAdJob.insertAttributeInt( 'JobID', jobID )
    result = self.__checkAndPrepareJob( classAdJob, classAdReq, resultDict['Owner'],
                                        resultDict['OwnerDN'], resultDict['OwnerGroup'],
                                        resultDict['DIRACSetup'] )

    if not result['OK']:
      jobAttrNames.append('Status')
      jobAttrValues.append('Failed')

      jobAttrNames.append('MinorStatus')
      jobAttrValues.append(result['Message'])

      resultInsert = self._insert( 'Jobs', jobAttrNames, jobAttrValues )
      if not resultInsert['OK']:
        return resultInsert

      retVal['Status'] = 'Failed'
      retVal['MinorStatus'] = result['Message']
      return retVal

    priority      = classAdJob.getAttributeInt( 'Priority' )
    jobAttrNames.append( 'UserPriority' )
    jobAttrValues.append( priority )

    site = classAdJob.getAttributeString('Site')
    if not site:
      site = 'ANY'
    jobAttrNames.append( 'Site' )
    jobAttrValues.append( site )

    jobAttrNames.append('Status')
    jobAttrValues.append('Received')

    jobAttrNames.append('MinorStatus')
    jobAttrValues.append('Job Rescheduled')

    jobAttrNames.append('ApplicationStatus')
    jobAttrValues.append('Unknown')

    jobAttrNames.append('ApplicationNumStatus')
    jobAttrValues.append(0)

    jobAttrNames.append('LastUpdateTime')
    jobAttrValues.append(Time.toString())

    reqJDL = classAdReq.asJDL()
    classAdJob.insertAttributeInt( 'JobRequirements', reqJDL )

    jobJDL = classAdJob.asJDL()

    result = self.setJobJDL( jobID, jobJDL )
    if not result['OK']:
      return result

    result = self.__setInitialJobParameters(classAdJob,jobID)
    if not result['OK']:
      return result

    result = self.setJobAttributes( jobID, jobAttrNames, jobAttrValues )
    if not result['OK']:
      return result

    retVal['InputData'] = classAdJob.lookupAttribute("InputData")
    retVal['RescheduleCounter'] = rescheduleCounter
    retVal['Status'] = 'Received'
    retVal['MinorStatus'] = 'Job Rescheduled'

    return retVal

#############################################################################
  def getSiteMask(self,siteState='Active'):
    """ Get the currently active site list
    """

    if siteState == "All":
      cmd = "SELECT Site FROM SiteMask"
    else:
      cmd = "SELECT Site FROM SiteMask WHERE Status='%s'" % siteState

    result = self._query( cmd )
    siteList = []
    if result['OK']:
      siteList = [ x[0] for x in result['Value']]

    return S_OK(siteList)

#############################################################################
  def setSiteMask(self,siteMaskList,authorDN='Unknown',comment='No comment'):
    """ Set the Site Mask to the given mask in a form of a list of tuples (site,status)
    """

    for site,status in siteMaskList:
      result = self.__setSiteStatusInMask(site,status,authorDN,comment)
      if not result['OK']:
        return result

    return S_OK()

#############################################################################
  def __setSiteStatusInMask(self,site,status,author,comment):
    """  Set the given site status to 'status' or add a new active site
    """

    req = "SELECT Status FROM SiteMask WHERE Site='%s'" % site
    result = self._query(req)
    if result['OK']:
      if len(result['Value']) > 0:
        current_status = result['Value'][0][0]
        if current_status == status:
          return S_OK()
        else:
          req =  "UPDATE SiteMask SET Status='%s',LastUpdateTime=UTC_TIMESTAMP()," \
                 "Author='%s', Comment='%s' WHERE Site='%s'"
          req = req % (status,author,comment,site)
      else:
        req = "INSERT INTO SiteMask VALUES ('%s','%s',UTC_TIMESTAMP(),'%s','%s')" % (site,status,author,comment)
      result = self._update(req)
      if not result['OK']:
        return S_ERROR('Failed to update the Site Mask')
      # update the site mask logging record
      req = "INSERT INTO SiteMaskLogging VALUES ('%s','%s',UTC_TIMESTAMP(),'%s','%s')" % (site,status,author,comment)
      result = self._update(req)
      if not result['OK']:
        self.log.warn('Failed to update site mask logging for %s' % site)
    else:
      return S_ERROR('Failed to get the Site Status from the Mask')

    return S_OK()

#############################################################################
  def banSiteInMask(self,site,authorDN='Unknown',comment='No comment'):
    """  Forbid the given site in the Site Mask
    """

    result = self.__setSiteStatusInMask(site,'Banned',authorDN,comment)
    return result

#############################################################################
  def allowSiteInMask(self,site,authorDN='Unknown',comment='No comment'):
    """  Forbid the given site in the Site Mask
    """

    result = self.__setSiteStatusInMask(site,'Active',authorDN,comment)
    return result

#############################################################################
  def removeSiteFromMask(selfself,site):
    """ Remove the given site from the mask
    """

    if site == "All":
      req = "DELETE FROM SiteMask"
    else:
      req = "DELETE FROM SiteMask WHERE Site='%s'" % site
    return self._update(req)

#############################################################################
  def getSiteMaskLogging(self,siteList):
    """ Get the site mask logging history for the list if site names
    """

    siteString = ','.join(siteList)
    req = "SELECT Site,Status,UpdateTime,Author,Comment FROM SiteMaskLogging WHERE Site in (%s)" % siteString
    req += " ORDER BY UpateTime ASC"
    result = self._query(req)
    if not result['OK']:
      return result

    resultDict = {}
    if not result['Value']:
      return S_OK(resultDict)

    for row in result['Value']:
      site,status,utime,author,comment = row
      if not resultDict.has_key(site):
        resultDict[site] = []
      resultDict[site].append((status,str(utime),author,comment))

    return S_OK(resultDict)

#############################################################################
  def __addQueue (self, requirements="[Requirements=true;]", priority=0):
    """ Add unconditionally a new Queue to the list of Task Queues with the given
        requirements and priority. The requirements are provided as a JDL snippet
    """

    self.log.info( 'JobDB.__addQueue: Adding new Task Queue with requirements' )
    self.log.info( 'JobDB.__addQueue: %s' % requirements )

    classAdQueue = ClassAd(requirements)
    if classAdQueue.isOK():
      reqJDL = classAdQueue.asJDL()
      res = self._getConnection()
      if not res['OK']:
        return res
      connection = res['Value']

      cmd = 'INSERT INTO TaskQueues (Requirements, Priority) '\
            ' VALUES (\'%s\', \'%s\' )' \
            % ( reqJDL, priority )
      result = self._update( cmd, connection )
      if not result['OK']:
        connection.close()
        return result
      result = self._query( 'SELECT LAST_INSERT_ID()', connection )
      connection.close()
      if not result['OK']:
        return result

      queueID = int(result['Value'][0][0])
      return S_OK(queueID)
    else:
      return S_ERROR('JobDB.__addQueue: Invalid requirements JDL')

#############################################################################
  def deleteQueue(self,queueID):
    """ Delete a Task Queue with queueID
    """
    req = "DELETE FROM TaskQueues WHERE TaskQueueId=%d" % queueID
    result = self._update(req)
    return result

#############################################################################
  def selectQueue(self, requirements):
    """  Select a queue with the given requirements or add a new one if it
         is not yet available. Requirements are provided as a value of the
         JDL Requirements attribute
    """

    res = self._getFields('TaskQueues',['Requirements','TaskQueueId'],[],[])
    if not res['OK']:
      return res
    classAdJob = ClassAd( '[ Requirements = %s ]' % requirements )
    jobRequirement = classAdJob.get_expression("Requirements").upper()
    for row in res['Value']:
      classAdQueue = ClassAd(row[0])
      queueID = row[1]
      if not classAdQueue.isOK():
        cmd = 'DELETE from TaskQueues WHERE TaskQueueId=\'%s\'' % queueID
        self._update( cmd )
      else:
        queueRequirement = classAdQueue.get_expression("Requirements")
        if queueRequirement.upper() == jobRequirement:
          return S_OK( queueID )

    self.log.info( 'JobDB.selectQueue: creating a new Queue' )
    return self.__addQueue( '[ Requirements = %s ]' % requirements )

#############################################################################
  def getTaskQueues(self):
    """ Get all the Task Queue requirements ordered descendingly by their
        priorities
    """

    req = "select TaskQueueId,Requirements,Priority from TaskQueues order by Priority DESC"
    result = self._query(req)
    if not result['OK']:
      return result

    return S_OK(result['Value'])

#############################################################################
  def addJobToQueue(self,jobID,queueID,rank):
    """Add the job specified by <jobID> to the Task Queue specified by
       <queueID> with the job rank <rank>
    """

    self.log.verbose('JobDB.addJobToQueue: Adding job %s to queue %s' \
                  ' with rank %s' % ( jobID, queueID, rank ) )

    cmd = 'INSERT INTO TaskQueue(TaskQueueId, JobID, Rank) ' \
          'VALUES ( %d, %d, %d )' % ( int(queueID), int(jobID), int(rank) )

    result = self._update( cmd )
    if not result['OK']:
      self.log.error("Failed to add job "+str(jobID)+" to the Task Queue")
      return result

    cmd = "UPDATE TaskQueues SET NumberOfJobs = NumberOfJobs + 1 WHERE TaskQueueId=%d" % queueID
    result = self._update( cmd )
    if not result['OK']:
      self.log.error("Failed to increment the job counter for the Task Queue %d" % queueID)
      return result

    # Check the Task Queue priority and adjust if necessary
    cmd = "SELECT Priority FROM TaskQueues WHERE TaskQueueId=%s" % queueID
    result = self._query(cmd)
    if not result['OK']:
      self.log.error("Failed to get priority of the TaskQueue "+str(queueID))
      return result

    old_priority = int(result['Value'][0][0])
    if rank > old_priority:
      cmd = "UPDATE TaskQueues SET Priority=%s WHERE TaskQueueId=%s" % (rank,queueID)
      result = self._update(cmd)
      if not result['OK']:
        self.log.error("Failed to update priority of the TaskQueue "+str(queueID))
        return result

    return S_OK()

 #############################################################################
  def lookUpJobInQueue(self,jobID):
    """ Check if the job with jobID is in the Task Queue
    """

    req = "SELECT * FROM TaskQueue WHERE JobId=" + str(jobID)
    result = self._query(req)
    if result['OK']:
      if result['Value']:
        return jobID

    return 0

 #############################################################################
  def getJobsInQueue(self,queueID):
    """ Get job IDs from the Task Queue with queueID ordered by their
        priorities
    """
    req = "SELECT JobID FROM TaskQueue WHERE TaskQueueId="+ str(queueID)+ \
          " ORDER BY Rank DESC, JobId"
    result = self._query(req)
    if result['OK']:
      if result['Value']:
        jobList = [x[0] for x in result['Value']]
        return S_OK(jobList)
      else:
        return S_OK([])
    else:
      return result

#############################################################################
  def getTaskQueueReport(self,queueList):
    """ Get the report of the Task Queue state:
        number of jobs per queue and queue priorities
    """
    if not queueList:
      req =  "SELECT TaskQueueId,NumberOfJobs,Priority FROM TaskQueues"
    else:
      idstring = string.join([str(x) for x in queueList],',')
      req = "SELECT TaskQueueId,NumberOfJobs,Priority FROM TaskQueues WHERE TaskQueueId in ( "+idstring+" )"

    result = self._queue(req)
    if result['OK']:
      return S_OK(result['Value'])
    else:
      return S_ERROR('Can not access the Task Queue tables')


#############################################################################
  def deleteJobFromQueue(self,jobID):
    """Delete the job specified by jobID from the Task Queue
    """

    self.log.info('JobDB: Deleting job %d from the Task Queue' % int(jobID) )

    req = "SELECT TaskQueueID FROM TaskQueue WHERE JobID=%d" % int(jobID)
    result = self._query(req)
    if not result['OK']:
      return result
    if len(result['Value']) > 0:
      queueID = int(result['Value'][0][0])
    else:
      return S_OK()

    cmd = "DELETE FROM TaskQueue WHERE JobID=%d" % int(jobID)
    result = self._update(cmd)
    if not result['OK']:
      return result

    cmd = "UPDATE TaskQueues SET NumberOfJobs = NumberOfJobs - 1 WHERE TaskQueueId=%d" % queueID
    result = self._update( cmd )
    if not result['OK']:
      self.log.error("Failed to decrement the job counter for the Task Queue %d" % queueID)
      return result

    # Check that the queue is empty and remove it eventually
    req = "SELECT TaskQueueID FROM TaskQueue WHERE TaskQueueID=%d" % int(queueID)
    result = self._query(req)
    if result['OK']:
      if len(result['Value']) == 0:
        req = "SELECT Requirements FROM TaskQueues WHERE TaskQueueID=%d" % int(queueID)
        result = self._query(req)
        if result['OK']:
          if len(result['Value']) > 0:
            requirements = result['Value'][0][0]
            self.log.info('JobDB: Removing Task Queue with requirements:' )
            self.log.info(requirements)
            req = "DELETE FROM TaskQueues WHERE TaskQueueID=%d" % int(queueID)
            result = self._update(req)
            return result
          else:
            self.log.warn('JobDB: Error while removing empty Task Queue' )
        else:
          self.log.warn('JobDB: Error while removing empty Task Queue' )

    return S_OK()


#############################################################################
  def setSandboxReady(self,jobID,stype='InputSandbox'):
    """ Set the sandbox status ready for the job with jobID
    """

    if stype == "InputSandbox":
      field = "ISandboxReadyFlag"
    elif stype == "OutputSandbox":
      field = "OSandboxReadyFlag"
    else:
      return S_ERROR('Illegal Sandbox type: '+stype)

    cmd = "UPDATE Jobs SET %s='True' WHERE JobID=%d" % (field, int(jobID))
    result = self._update(cmd)
    return result


##########################################################################################
#  def updateRankInTaskQueue(self,jobID,newRank):
#    """ Update the rank of a job specified by its jobID to newRank
#    """
#
#    cmd = "UPDATE TaskQueue SET Rank=%d, WHERE JobID=%d;' % (newRank,jobID)
#    return self._update( cmd )

##########################################################################################
#  def getJobsFromTaskQueue(self):
#    """  Get all the jobs from the Task Queue
#    """
#
#    cmd = 'SELECT * FROM TaskQueue'
#    try:
#      result = self._query( cmd )
#      if result['OK']:
#        return result['Value']
#      else: return S_ERROR('Cannot get jobs from the TaskQueue table')
#    except:
#      return S_ERROR('Cannot connect to the mysql')

##########################################################################################
  def getCounters(self, attrList, condDict, cutDate):
    """ Count the number of jobs on each distinct combination of AttrList, selected
        with condition defined by condDict and cutDate
    """
    cond = self.__buildCondition( condDict, newer=cutDate )
    attrNames = string.join(map(lambda x: str(x),attrList ),',')
    cmd = 'SELECT %s FROM Jobs %s' % ( attrNames, cond )
    result = self._query( cmd )
    if not result['OK']:
      return result
    countTreeDict = {}
    for value in result['Value']:
      currentDict = countTreeDict
      for i in range(len(attrList)):
        if not currentDict.has_key(value[i]):
          currentDict[value[i]]={}
        currentDict = currentDict[value[i]]
      if currentDict == {}:
        currentDict['Counter'] = 0
      currentDict['Counter'] += 1
      currentDict = countTreeDict
    counterList = []

    distinctAttributesList = []
    currentDict = countTreeDict
    for index in range(len(attrList)):
      if not distinctAttributesList:
        # for the attribute in the List distinctAttributesList is empty
        for attr in currentDict.keys():
          distinctAttributesList.append([attr])
      else:
        newdistinctAttributesList = []
        for attrs in distinctAttributesList:
          currentDict = countTreeDict
          for i in range(index):
            currentDict = currentDict[attrs[i]]
          for attr in currentDict.keys():
            if index+1 == len(attrList):
              # Now the List must become a Dictionary and the counter added
              counter = currentDict[attr]['Counter']
              newDict = {}
              for i in range(len(attrList)):
                # FIXME: JobDB default values None can not be marshall by XML-RPC
                # we should fixed a better default; ie, "Unknown"
                if ( attrs + [attr] )[i]:
                  newDict[attrList[i]] = ( attrs + [attr] )[i]
                else:
                  newDict[attrList[i]] = 'Unknown'
              newdistinctAttributesList.append( [newDict, counter] )
            else:
              newdistinctAttributesList.append( attrs + [attr] )
          distinctAttributesList = list(newdistinctAttributesList)

    return S_OK(distinctAttributesList)

#################################################################################
  def getSiteSummary(self):
    """ Get the summary of jobs in a given status on all the sites
    """

    waitingList = ['Submitted','Assigned','Waiting','Matched']
    waitingString = ','.join(["'"+x+"'" for x in waitingList])

    result = self.getDistinctJobAttributes('Site')
    if not result['OK']:
      return result

    siteList = result['Value']
    siteDict = {}
    totalDict = {'Waiting':0,'Running':0,'Stalled':0,'Done':0,'Failed':0}

    for site in siteList:
      if site == "ANY":
        continue
      # Waiting
      siteDict[site] = {}
      req = "SELECT COUNT(JobID) FROM Jobs WHERE Status IN (%s) AND Site='%s'" % (waitingString,site)
      result = self._query(req)
      if result['OK']:
        count = result['Value'][0][0]
      else:
        return S_ERROR('Failed to get Site data from the JobDB')
      siteDict[site]['Waiting'] = count
      totalDict['Waiting'] += count
      # Running,Stalled,Done,Failed
      for status in ['Running','Stalled','Done','Failed']:
        req = "SELECT COUNT(JobID) FROM Jobs WHERE Status='%s' AND Site='%s'" % (status,site)
        result = self._query(req)
        if result['OK']:
          count = result['Value'][0][0]
        else:
          return S_ERROR('Failed to get Site data from the JobDB')
        siteDict[site][status] = count
        totalDict[status] += count

    siteDict['Total'] = totalDict
    return S_OK(siteDict)

#####################################################################################
  def setHeartBeatData(self,jobID,staticDataDict, dynamicDataDict):
    """ Add the job's heart beat data to the database
    """

    # Set the time stamp first
    req = "UPDATE Jobs SET HeartBeatTime=UTC_TIMESTAMP() WHERE JobID=%d" % jobID
    result = self._update(req)
    if not result['OK']:
      return S_ERROR('Failed to set the heart beat time: '+result['Message'])

    ok = True
    # FIXME: It is rather not optimal to use parameters to store the heartbeat info, must find a proper solution
    # Add static data items as job parameters
    result = self.setJobParameters( jobID, staticDataDict.items() )
    if not result['OK']:
      ok = False
      self.log.warn(result['Message'])

    # Add dynamic data to the job heart beat log
    for key,value in dynamicDataDict.items():
      result = self._escapeString(value)
      if not result['OK']:
        self.log.warn('Failed to escape string '+value)
        continue
      e_value = result['Value']
      req = "INSERT INTO HeartBeatLoggingInfo (JobID,Name,Value,HeartBeatTime) "
      req += "VALUES (%d,'%s','%s',UTC_TIMESTAMP())" % (jobID,key,e_value)
      result = self._update(req)
      if not result['OK']:
        ok = False
        self.log.warn(result['Message'])

    if ok:
      return S_OK()
    else:
      return S_ERROR('Failed to store some or all the parameters')

#####################################################################################
  def getHeartBeatData(self,jobID):
    """ Retrieve the job's heart beat data
    """
    cmd = 'SELECT Name,Value,HeartBeatTime from HeartBeatLoggingInfo WHERE JobID=%d' % (int(jobID))
    res = self._query( cmd )
    if not res['OK']:
      return res

    if len(res['Value']) == 0:
      return S_OK ([])

    result = []
    values = res['Value']
    for row in values:
      result.append((str(row[0]),'%.01f' %(float(row[1].replace('"',''))),str(row[2])))

    return S_OK(result)

#####################################################################################
  def setJobCommand(self,jobID,command,arguments=''):
    """ Store a command to be passed to the job together with the
        next heart beat
    """

    req = "INSERT INTO JobCommands (JobID,Command,Arguments,ReceptionTime) "
    req += "VALUES (%d,'%s','%s',UTC_TIMESTAMP())" % (jobID,command, arguments)
    result = self._update(req)
    return result

 #####################################################################################
  def getJobCommand(self,jobID,status='Received'):
    """ Get a command to be passed to the job together with the
        next heart beat
    """

    req = "SELECT Command, Arguments FROM JobCommands WHERE JobID=%d AND Status='%s'" % (jobID,status)
    result = self._query(req)
    if not result['OK']:
      return result

    resultDict = {}
    if result['Value']:
      for row in result['Value']:
        resultDict[row[0]] = row[1]

    return S_OK(resultDict)

#####################################################################################
  def setJobCommandStatus(self,jobID,command,status):
    """ Set the command status
    """

    req = "UPDATE JobCommands SET Status='%s' WHERE JobID=%d AND Command='%s'" % (status,jobID,command)
    result = self._update(req)
    return result

#####################################################################################
  def getSummarySnapshot( self ):
    """ Get the summary snapshot for a given combination
    """
    defFields = [ 'DIRACSetup', 'Status', 'MinorStatus', 'ApplicationStatus',
                  'Site', 'Owner', 'OwnerGroup', 'JobGroup', 'JobSplitType' ]
    valueFields = [ 'COUNT(JobID)', 'SUM(RescheduleCounter)' ]
    defString = ", ".join( defFields )
    valueString = ", ".join( valueFields )
    sqlCmd = "SELECT %s, %s From Jobs GROUP BY %s" % ( defString, valueString, defString )
    result = self._query( sqlCmd )
    if not result[ 'OK' ]:
      return result
    return S_OK( ( ( defFields + valueFields ), result[ 'Value' ] ) )
