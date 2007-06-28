########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/DB/JobDB.py,v 1.7 2007/06/28 14:25:30 atsareg Exp $
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

__RCSID__ = "$Id: JobDB.py,v 1.7 2007/06/28 14:25:30 atsareg Exp $"

import re, os, sys, string
import time
import threading

from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd
from types                                     import *
from DIRAC                                     import gLogger, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Config import gConfig
from DIRAC.Core.Base.DB import DB


#############################################################################
class JobDB(DB):

  def __init__( self, maxQueueSize=10 ):
    """ Standard Constructor
    """

    DB.__init__(self,'JobDB','WorkloadManagement/JobDB',maxQueueSize)

    self.maxRescheduling = 30
    result = gConfig.getOption( self.cs_path+'/MaxRescheduling')
    if not result['OK']:
      self.log.error('Failed to get the MaxRescheduling limit')
      self.log.error('Using default value '+str(self.maxRescheduling))
    else:
      self.maxRescheduling = int(result['Value'])


    self.jobAttributes = []
    self.getIdLock = threading.Lock()

    res = self.__getAttributeNames()

    if not res['OK']:
      err = 'Can not retrieve job Attributes.'
      self.log.fatal( 'JobDB: %s' % err )
      sys.exit( err )
      return

    self.nJobAttributeNames = len(self.jobAttributeNames)

    self.log.always("MaxReschedule:  "+`self.maxRescheduling`)
    self.log.always("==================================================")

  def dumpParameters(self):
    """  Dump the JobDB connection parameters to the stdout
    """

    print "=================================================="
    print "SystemSet:", self.system
    print "User:", self.dbUser
    print "Host:", self.dbHost
    print "Password", self.dbPass
    print "DBName", self.dbName
    print "MaxQueue", self.maxQueueSize

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

    return S_OK( )

  def __buildCondition(self, condDict, older=None, newer=None ):
    """ build SQL condition statement from provided condDict
        and other extra conditions
    """
    condition = ''
    conjonction = "WHERE"

    if condDict != None:
      for attrName, attrValue in condDict.items():
        condition = ' %s %s %s=\'%s\'' % ( condition,
                                           conjonction,
                                           str(attrName),
                                           str(attrValue)  )
        conjonction = "AND"

    if older:
      condition = ' %s %s LastUpdateTime<\'%s\'' % ( condition,
                                                 conjonction,
                                                 str(older) )
      conjonction = "AND"

    if newer:
      condition = ' %s %s LastUpdateTime>\'%s\'' % ( condition,
                                                 conjonction,
                                                 str(newer) )

    return condition


#############################################################################
  def getJobID(self):
    """Get the next unique JobID and prepare the new job insertion
    """
    # if jobid it is put not in the JDL by the client this would be an internal method
    # the id is unique per connection, thus to assure teh uniqueness in a multithreaded
    # server a new connection should be established here or in the client. or the server
    # should make sure that only 1 thread is allowed to call this method at any time.
    # for the moment I put a lock here.

    self.getIdLock.acquire()

    cmd = 'INSERT INTO Jobs (SubmissionTime) VALUES (CURDATE())'

    err = 'JobDB.getJobID: Failed to retrieve a new Id.'

    res = self._update( cmd )
    if not res['OK']:
      self.getIdLock.release()
      return S_ERROR( '1 %s\n%s' % (err, res['Message'] ) )

    cmd = 'SELECT MAX(JobID) FROM Jobs'
    res = self._query( cmd )
    if not res['OK']:
      self.getIdLock.release()
      return S_ERROR( '2 %s\n%s' % (err, res['Message'] ) )

    try:
      jobID = int(res['Value'][0][0])
      self.log.info( 'JobDB: New JobID served "%s"' % jobID )
    except Exception, x:
      self.getIdLock.release()
      return S_ERROR( '3 %s\n%s' % (err, str(x) ) )

    self.getIdLock.release()

    return S_OK( jobID )


#############################################################################
  def getAttributesForJobList(self,jobIDList,attrList):
    """ Get attributes for the jobs in the the jobIDList.
        Returns an S_OK structure with a dictionary of dictionaries os its Value:
        ValueDict[jobID][attribute_name] = attribute_value
    """
    attrNames = string.join(map(lambda x: str(x),attrList ),',')
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
        for i in range(len(attrList)):
          try:
            jobDict[attrList[i]] = attrValues[i].tostring()
          except:
            jobDict[attrList[i]] = str(attrValues[i])
        retDict[int(jobID)] = jobDict
      return S_OK( retDict )
    except Exception,x:
      return S_ERROR( 'JobDB.getJobsAttributes: Failed\n%s'  % str(x) )


#############################################################################
  def getDistinctJobAttributes(self,attribute, condDict = {}, older = None, newer=None):
    """ Get distinct values of the job attribute under specified conditions
    """
    cmd = 'SELECT  DISTINCT(%s) FROM Jobs' % attribute

    cond = self.__buildCondition( condDict, older=older, newer=newer )

    res = self._query( cmd + cond )
    if not res['OK']:
      return res

    return S_OK( map( self._to_value, res['Value'] ) )

#############################################################################
  def getAllJobParameters(self, jobID):
    """ Get all Job Parameters defined for jobID.
        Returns a dictionary with all Job Parameters.
        Returns an empty dictionary if no matching job found.
    """

    self.log.debug( 'JobDB.getParameters: Getting Parameters for job %s' %jobID )
    result = self._getFields( 'JobParameters',
                           ['Name', 'Value'],
                           ['JobID'], [jobID])
    if not result['OK']:
      return result

    parameters = {}
    for name,value in result['Value']:
      try:
        parameters[name] = value.tostring()
      except:
        parameters[name] = value

    return S_OK( parameters )

#############################################################################
  def getAllJobAttributes(self, jobID):
    """ Get all Job Attributes for a given jobID.
        Return a dictionary with all Job Attributes,
        return an empty dictionary if matching job found
    """

    self.log.debug( 'JobDB.getAllJobAttributes: Getting Attributes for job = "%s".' %jobID )
    cmd = 'SELECT * FROM Job WHERE JobID=\'%s\'' % jobID
    res = self._query( cmd )
    if not res['OK']:
      return res

    if len(res['Value']) == 0:
      return S_OK ( {} )

    values = res['Value'][0]
    if not len(values) == self.nJobAttributeNames:
      return S_ERROR( 'Error in JobDB schema.' )

    attributes = {}
    for i in range(len(self.jobAttributeNames)):
      attributes[self.jobAttributeNames[i]] = str(values[i])

    return S_OK( attributes )

#############################################################################
  def getJobParameters( self, jobID, parameters ):
    """ Get parameters for job specified by jobID. Parameters can be
        either job attributes ( fields in the Jobs table ) or those
        stored in the JobParameters table.
        The return value is a dictionary of the structure:
        Dict[Name] = Value
    """

    jparameters = []
    jattributes = []
    for p in parameters:
      if p in self.jobAttributeNames:
        jattributes.append(p)
      else:
        jparameters.append(p)

    result = {}

    # Get Job Attributes first
    if jattributes:
      res = self._getFields( 'Jobs', jattributes, ['JobID'], [jobID] )
      if not res['OK']:
        return res
      if len(res['Value']) > 0:
        for i in range(len(jattributes)):
          name = jattributes[i]
          try:
            value = res['Value'][0][i].tostring()
          except:
            value = str(res['Value'][0][i])
          result[name] = value
      else:
        return S_ERROR('Job '+str(jobID)+' not found')

    # Get Job Parameters
    for i in range(len(jparameters)):
      name = jparameters[i]
      res = self._getFields('JobParameters',['Value'],['JobID','Name'],[jobID,name])
      if not res['OK']:
        return res
      try:
        value = res['Value'][0][0].tostring()
      except:
        value = 'Unknown'

      result[name] = value

    return S_OK(result)

#############################################################################
  def getJobAttribute(self,jobID,attribute):
    """ Get the given attribute of a job specified by its jobID
    """

    result = self.getJobParameters(jobID,[attribute])
    if result['OK']:
      value = result['Value'][attribute]
      return S_OK(value)
    else:
      return result

#############################################################################
  def getInputData (self, jobID):
    """Get input data

       Get input data for the given job
    """

    cmd = 'SELECT LFN FROM InputData WHERE JobID=\'%s\'' %jobID
    res = self._query(cmd)
    if not res['OK']:
      return res

    return S_OK( map( self._to_value, res['Value'] ) )


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
  def selectJobs(self, condDict, older=None, newer=None, ordered=None, limit=None ):
    """ Select jobs matching the following conditions:
        - condDict dictionary of required Key = Value pairs;
        - with the last update date older and/or newer than given dates;

        The result is ordered by JobID if requested, the result is limited to a given
        number of jobs if requested.
    """

    self.log.debug( 'JobDB.selectJobs: retrieving jobs.' )

    condition = self.__buildCondition(condDict, older, newer)

    if ordered:
      condition = condition + ' Order by JobID'

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
  def setJobAttribute(self, jobID, attrName, attrValue, update=False ):
    """ Set an attribute value for job specified by jobID.
        The LastUpdate time stamp is refreshed if explicitely requested
    """

    if update:
      cmd = 'UPDATE Jobs SET %s=\'%s\',LastUpdateTime=CURDATE() WHERE JobID=\'%s\'' % ( attrName, attrValue, jobID )
    else:
      cmd = 'UPDATE Jobs SET %s=\'%s\' WHERE JobID=\'%s\'' % ( attrName, attrValue, jobID )

    res = self._update( cmd )
    if res['OK']:
      return res
    else:
      return S_ERROR( 'JobDB.setAttribute: failed to set attribute' )

#############################################################################
  def setJobStatus(self,jobID,status='',minor='',application='',appCounter=None):
    """ Set status of the job specified by its jobID
    """

    if status:
      result = self.setJobAttribute(jobID,'Status',status,update=True)
      if not result['OK']:
        return result
    if minor:
      result = self.setJobAttribute(jobID,'MinorStatus',minor,update=True)
      if not result['OK']:
        return result
    if application:
      result = self.setJobAttribute(jobID,'ApplicationStatus',application,update=True)
      if not result['OK']:
        return result
    if appCounter:
      result = self.setJobAttribute(jobID,'ApplicationCounter',appCounter,update=True)
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

    cmd = 'INSERT INTO JobParameters VALUES(\'%s\', \'%s\', \'%s\' )' % ( jobID, key, value )
    res = self._update( cmd )
    if not res['OK']:
      result = S_ERROR('JobDB.setJobParameter: operation failed.')

    return res

#############################################################################
  def setAtticJobParameter(self,jobID,key,value,rescheduleCounter):
    """ Set attic parameter for job specified by its jobID when job rescheduling
        for later debugging
    """

    cmd = 'INSERT INTO AtticJobParameters VALUES(%d,%d,\'%s\',\'%s\')' % \
         (int(jobID),rescheduleCounter,key,value)
    res = self._update( cmd )
    if not res['OK']:
      result = S_ERROR('JobDB.setAtticJobParameter: operation failed.')

    return res

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
    for key,value in parameters.items():
      res = self.setJobParameter( jobID, key, value )
      if not res['OK']:
        return res


    return S_OK()

#############################################################################
  def setJobJDL(self, jobID, JDL=None, originalJDL = None):
    """ Insert JDL's for job specified by jobID
    """

    req = "SELECT OriginalJDL FROM JobJDLs WHERE JobID=%d" % int(jobID)
    result = self._query(req)
    updateFlag = False
    if result['OK']:
      if len(result['Value']) > 0:
        updateFlag = True

    if JDL:

      if updateFlag:
        cmd = "UPDATE JobJDLs Set JDL='%s' WHERE JobID=%d" % (JDL,jobID)
      else:
        cmd = "INSERT INTO JobJDLs (JobID,JDL) VALUES (%d,'%s')" % (jobID,JDL)
      result = self.jobDB._update(cmd)
      if not result['OK']:
        return result
    if originalJDL:
      if updateFlag:
        cmd = "UPDATE JobJDLs Set OriginalJDL='%s' WHERE JobID=%d" % (originalJDL,jobID)
      else:
        cmd = "INSERT INTO JobJDLs (JobID,OriginalJDL) VALUES (%d,'%s')" % (jobID,originalJDL)

      result = self._update(cmd)

    return result

#############################################################################
  def getJobJDL(self,jobID,original=False):
    """ Get JDL for job specified by its jobID. By default the current job JDL
        is returned. If 'original' argument is True, original JDL is returned
    """

    if original:
      cmd = "SELECT OriginalJDL FROM JobJDLs WHERE JobID=%d" % int(jobID)
    else:
      cmd = "SELECT JDL FROM JobJDLs WHERE JobID=%d" % int(jobID)

    print cmd
    result = self._query(cmd)
    if result['OK']:
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

    return self.setJobStatus(jobID,status='received',minor='Initial insertion')

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

    print "JobDB:",jdl

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
      classadJob.set_expression("Requirements", "true")

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
      cmd = 'INSERT INTO InputData (JobID,LFN) VALUES (\'%s\', \'%s\' )' % ( jobID, lfn )
      res = self._update( cmd )
      if not res['OK']:
        return res

    result = self.__setInitialJobParameters(classadJob,jobID)
    if not result['OK']:
      return result

    result = self.__setInitialSite(classadJob,jobID)
    if not result['OK']:
      return result

    result = self.setJobStatus(jobID,status='received',minor='Job accepted')
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

    # Check the Reschedule counter first
    rescheduleCounter = 0
    req = "SELECT RescheduleCounter from Jobs WHERE JobID=%s" % jobID
    result = self._query(req)
    if result['OK']:
      if result['Value']:
        rescheduleCounter = result['Value'][0][0]
      else:
        return S_ERROR('Job '+str(jobID)+' not found in the system')

    # Exit if the limit of the reschedulings is reached
    if rescheduleCounter >= self.maxRescheduling:
      self.log.error('Maximum number of reschedulings is reached for job %s' % jobID)
      res = self.setJobStatus(jobID, status='failed',update=True)
      res = self.setJobStatus(jobID, application='Maximum of reschedulings reached')
      return S_ERROR('Maximum number of reschedulings is reached: %s' % self.maxRescheduling)

    req = "UPDATE Jobs set RescheduleCounter=RescheduleCounter+1 WHERE JobID=%s" % jobID
    res = self._update(req)
    if not res['OK']:
      return res

    # Save the job parameters for later debugging
    result = self.getAllJobParameters(jobID)
    if result['OK']:
      parDict = result['Value']
      for key,value in parDict.items():
        result = self.setAtticJobParameter(jobID,key,value,rescheduleCounter)
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
    classadJob = ClassAd(jdl)
    res = self.__setInitialJobParameters(classadJob,jobID)
    if not res['OK']:
      return res

    res = self.setJobStatus(jobID,
                            status='received',
                            minor = 'Job Rescheduled',
                            application='Unknown',
                            appCounter=0)
    if not res['OK']:
      return res

    cmd = 'DELETE FROM TaskQueue WHERE JobID=\'%s\'' % jobID
    res = self._update( cmd )
    if not res['OK']:
      return S_ERROR("Failed to delete job from the Task Queue")

    result = S_OK()
    result['InputData']  = classadJob.lookupAttribute("InputData")
    result['JobID']  = jobID
    result['RescheduleCounter']  = rescheduleCounter+1
    return result

#############################################################################
  def getMask(self):
    """ Get the currently active site list
    """

    cmd = "SELECT Site FROM SiteMask WHERE Status='Active'"
    result = self._query( cmd )
    siteList = []
    if result['OK']:
      siteList = [ x[0] for x in result['Value']]

    if siteList:
      # Form the site mask as JDL
      mask = self.__getMaskJDL(siteList)
      return S_OK(mask)
    else:
      return S_ERROR('Failed to get site mask')

#############################################################################
  def __getMaskJDL(self,siteList):
    """ Create a Site Mask in a form of a JDL from the site list
    """

    mask = '[  Requirements = OtherSite == "'
    mask = mask + string.join(siteList,'" || Other.Site == "')
    mask = mask + '"   ]'
    return mask

#############################################################################
  def setMask(self,mask,authorDN='Unknown'):
    """ Set the Site Mask to the given mask in a form of JDL string or
        in a form of a site list
    """

    _mask = mask

    if type(mask) in StringTypes:
      classadMask = ClassAd(_mask)
      if classadMask.isOK():
        if  classadMask.lookupAttribute("Requirements"):
          requirements = classadMask.get_expression("Requirements")
        else:
          return S_ERROR("Empty mask")

        tmp_list = requirements.split('"')
        _mask = []
        for i in range(1,len(tmp_list),2):
          _mask.append(tmp_list[i])
      else:
        return S_ERROR('Invalid Site Mask')

    # Ban all the sites first
    req = "UPDATE SiteMask SET Status='Banned', LastUpdateTime=NOW(), Author='%s'"
    req = req % authorDN
    result = self._update(req)
    if not result['OK']:
      return result

    for site in _mask:
      result = self.allowSiteInMask(site)
      if not result['OK']:
        return result

    return S_OK()

#############################################################################
  def __setSiteStatusInMask(self,site,status,author):
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
          req =  "UPDATE SiteMask SET Status='%s',LastUpdateTime=NOW()," \
                 "Author='%s' WHERE Site='%s'"
          req = req % (status,author,site)
      else:
        req = "INSERT INTO SiteMask VALUES ('%s','%s',NOW(),'%s')" % (site,status,author)
      result = self._update(req)
      if not result['OK']:
        return S_ERROR('Failed to update the Site Mask')
      else:
        return S_OK()
    else:
      return S_ERROR('Failed to get the Site Status from the Mask')

#############################################################################
  def banSiteInMask(self,site,authorDN='Unknown'):
    """  Forbid the given site in the Site Mask
    """

    result = self.__setSiteStatusInMask(site,'Banned',authorDN)
    return result

#############################################################################
  def allowSiteInMask(self,site,authorDN='Unknown'):
    """  Forbid the given site in the Site Mask
    """

    result = self.__setSiteStatusInMask(site,'Active',authorDN)
    return result

#############################################################################
  def __addQueue (self, requirements="[Requirements=true;]", priority=0):
    """ Add unconditionally a new Queue to the list of Task Queues with the given
        requirements and priority. The requirements are provided as a JDL snippet
    """

    self.log.info( 'JobDB.__addQueue: Adding new Task Queue with requirements' )
    self.log.info( 'JobDB.__addQueue: %s' % requirements )

    classQueue = ClassAd(requirements)
    if classQueue.isOK():
      reqJDL = classQueue.asJDL()
      self.getIdLock.acquire()
      cmd = 'INSERT INTO TaskQueues (Requirements, Priority) '\
            ' VALUES (\'%s\', \'%s\' )' \
            % ( reqJDL, priority )
      result = self._update( cmd )
      if not result['OK']:
        self.getIdLock.release()
        return result
      result = self._query( 'SELECT LAST_INSERT_ID()' )
      self.getIdLock.release()
      if not result['OK']:
        return result

      queueId = int(result['Value'][0][0])
      return S_OK(queueId)
    else:
      return S_ERROR('JobDB.addQueue: Invalid requirements JDL')

#############################################################################
  def selectQueue(self, requirements):
    """  Select a queue with the given requirements or add a new one if it
         is not yet available. Requirements are provided as a value of the
         JDL Requirements attribute
    """

    res = self._getFields('TaskQueues',['Requirements','TaskQueueId'],[],[])
    if not res['OK']:
      return res
    for row in res['Value']:
      classadQueue = ClassAd(row[0])
      queueId = row[1]
      if not classadQueue.isOK():
        cmd = 'DELETE from TaskQueues WHERE TaskQueueId=\'%s\'' % queueId
        self._update( cmd )
      else:
        queueRequirement = classadQueue.get_expression("Requirements")
        classadJob = ClassAd('[ Requirements = '+requirements+' ]')
        jobRequirement = classadJob.get_expression("Requirements")
        if queueRequirement.upper() == jobRequirement.upper():
          return S_OK( queueId )

    self.log.info( 'JobDB.selectQueue: creating a new Queue' )
    return self.__addQueue( '[ Requirements = %s ]' % requirements )

#############################################################################
  def addJobToQueue(self,jobID,queueId,rank):
    """Add the job specified by <jobID> to the Task Queue specified by
       <queueId> with the job rank <rank>
    """

    self.log.info('JobDB.addJobToQueue: Adding job %s to queue %s' \
                  ' with rank %s' % ( jobID, queueId, rank ) )

    cmd = 'INSERT INTO TaskQueue(TaskQueueId, JobID, Rank) ' \
          'VALUES ( %d, %d, %d )' % ( int(queueId), int(jobID), int(rank) )

    result = self._update( cmd )
    if not result['OK']:
      self.log.error("Failed to add job "+str(jobID)+" to the Task Queue")
      return result

    # Check the Task Queue priority and adjust if necessary
    cmd = "SELECT Priority FROM TaskQueues WHERE TaskQueueId=%s" % queueId
    result = self._query(cmd)
    if not result['OK']:
      self.log.error("Failed to get priority of the TaskQueue "+str(queueId))
      return result

    old_priority = int(result['Value'][0][0])
    if rank > old_priority:
      cmd = "UPDATE TaskQueues SET Priority=%s WHERE TaskQueueId=%s" % (rank,queueId)
      result = self._update(cmd)
      if not result['OK']:
        self.log.error("Failed to update priority of the TaskQueue "+str(queueId))
        return result

    return S_OK()

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
            self.log.error('JobDB: Error while removing empty Task Queue' )
        else:
          self.log.error('JobDB: Error while removing empty Task Queue' )

    return S_OK()


#############################################################################
  def setSandboxReady(self,jobID,stype='Input'):
    """ Set the sandbox status ready for the job with jobID
    """

    if stype == "Input":
      field = "ISandboxReady"
    elif stype == "Output":
      field = "OSandboxReady"
    else:
      return S_ERROR('Illegal Sandbox type: '+stype)

    cmd = "UPDATE Jobs SET %s='True' WHERE JobID=%d" % (stype, int(jobID))
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
