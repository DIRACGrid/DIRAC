########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/DB/PilotAgentsDB.py,v 1.15 2008/03/07 17:17:03 atsareg Exp $
########################################################################
""" PilotAgentsDB class is a front-end to the Pilot Agent Database.
    This database keeps track of all the submitted grid pilot jobs.
    It also registers the mapping of the DIRAC jobs to the pilot
    agents.

    Available methods are:

    addPilotReference()
    setPilotStatus()
    deletePilot()
    clearPilots()
    getPilotOwner()
    setPilotDestinationSite()
    storePilotOutput()
    getPilotOutput()
    setJobForPilot()
    getPilotsForJob()
    getPilotOwner()
    getPilotsSummary()

"""

__RCSID__ = "$Id: PilotAgentsDB.py,v 1.15 2008/03/07 17:17:03 atsareg Exp $"

from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Base.DB import DB
from types import *

DEBUG = 1

#############################################################################
class PilotAgentsDB(DB):

  def __init__(self, maxQueueSize=10 ):

     print "Initializing PilotAgentsDB"

     DB.__init__(self,'PilotAgentsDB','WorkloadManagement/PilotAgentsDB',maxQueueSize)

     print "Initializing PilotAgentsDB - done"

##########################################################################################
  def addPilotReference(self,pilotRef,jobID,ownerDN,ownerGroup,broker='Unknown',
                        gridType='DIRAC',requirements='Unknown'):
    """ Add a new pilot job reference """

    result = self._escapeString(requirements)
    if not result['OK']:
      gLogger.warn('Failed to escape requirements string')
      e_requirements = "Failed to escape requirements string"
    e_requirements = result['Value']
    req = "INSERT INTO PilotAgents( PilotJobReference, InitialJobID, OwnerDN, " + \
          "OwnerGroup, Broker, GridType, SubmissionTime, LastUpdateTime, Status, GridRequirements ) " + \
          "VALUES ('%s',%d,'%s','%s','%s','%s',UTC_TIMESTAMP(),UTC_TIMESTAMP(),'Submitted','%s')" % \
          (pilotRef,int(jobID),ownerDN,ownerGroup,broker,gridType,e_requirements)
    return self._update(req)

##########################################################################################
  def setPilotStatus(self,pilotRef,status,destination=None,updateTime=None):
    """ Set pilot job LCG status """

    setList = []
    setList.append("Status='%s'" % status)
    if destination:
      setList.append("DestinationSite='%s'" % destination)
    if updateTime:
      setList.append("LastUpdateTime='%s'" % updateTime)
    else:
      setList.append("LastUpdateTime=UTC_TIMESTAMP()")

    set_string = ','.join(setList)
    req = "UPDATE PilotAgents SET "+set_string+" WHERE PilotJobReference='%s'" % pilotRef

    return self._update(req)

##########################################################################################
  def selectPilots(self,statusList=[],owner=None,ownerGroup=None):
    """ Select pilot references according to the provided criteria
    """

    req = "SELECT PilotJobReference from PilotAgents"

    # Build conditions
    condList = []

    if statusList:
      cList = ["'"+x+"'" for x in statusList]
      condList.append("Status IN (%s)" % ",".join(cList))
    if owner:
      condList.append("OwnerDN = '%s'" % owner)
    if ownerGroup:
      condList.append("OwnerGroup = '%s'" % ownerGroup)

    if condList:
      conditions = " AND ".join(condList)
      req += " WHERE "+conditions

    result = self._query(req)
    if not result['OK']:
      return result

    pilotList = []
    if result['Value']:
      pilotList = [ x[0] for x in result['Value']]

    return S_OK(pilotList)


##########################################################################################
  def deletePilot(self,pilotRef):
    """ Delete Pilot reference from the LCGPilots table """

    req = "DELETE FROM PilotAgents WHERE PilotJobReference='%s'" % pilotRef
    return self._update(req)

##########################################################################################
  def clearPilots(self,interval=20):
    """ Delete all the pilot references submitted before <interval> days """

    req = "DELETE FROM PilotAgents WHERE SubmissionTime < DATE_SUB(CURDATE(),INTERVAL %d DAY)" % interval
    return self._update(req)

##########################################################################################
  def getPilotInfo(self,pilotRef):
    """ Get all the information for the pilot job reference or reference list
    """

    parameters = ['PilotJobReference','OwnerDN','OwnerGroup','GridType','Broker',
                  'Status','DestinationSite','BenchMark']
    param_string = ','.join(parameters)

    list_type = False
    if type(pilotRef) == ListType:
      list_type = True
      if pilotRef:
        refString = ",".join(["'"+x+"'" for x in pilotRef])
        req = "SELECT "+param_string+" FROM PilotAgents WHERE PilotJobReference IN (%s)" % refString
      else:
        req = "SELECT "+param_string+" FROM PilotAgents"
    else:
      req = "SELECT "+param_string+" FROM PilotAgents WHERE PilotJobReference='%s'" % pilotRef

    print req

    result = self._query(req)
    if not result['OK']:
      return result
    else:
      if result['Value']:
        if list_type:
          resDict = {}
          for row in result['Value']:
            pilotDict = {}
            for i in range(len(parameters)-1):
              pilotDict[parameters[i+1]] = row[i+1]
            resDict[row[0]] = pilotDict
          return S_OK(resDict)
        else:
          pilotDict = {}
          for i in range(len(parameters)-1):
            pilotDict[parameters[i+1]] = result['Value'][0][i+1]
          return S_OK(pilotDict)
      else:
        return S_ERROR('PilotJobReference(s) not found: '+str(pilotRef))

##########################################################################################
  def setPilotDestinationSite(self,pilotRef,destination):
    """ Set the pilot agent destination site
    """

    req = "UPDATE PilotAgents SET DestinationSite='%s' WHERE PilotJobReference='%s'" % (destination,pilotRef)
    result = self._update(req)
    return result

 ##########################################################################################
  def setPilotBenchmark(self,pilotRef,mark):
    """ Set the pilot agent benchmark
    """

    req = "UPDATE PilotAgents SET BenchMark='%f' WHERE PilotJobReference='%s'" % (mark,pilotRef)
    result = self._update(req)
    return result

##########################################################################################
  def setPilotRequirements(self,pilotRef,requirements):
    """ Set the pilot agent grid requirements
    """

    result = self._escapeString(requirements)
    if not result['OK']:
      return S_ERROR('Failed to escape requirements string')
    e_requirements = result['Value']
    req = "UPDATE PilotAgents SET GridRequirements='%s' WHERE PilotJobReference='%s'" % (e_requirements,pilotRef)
    result = self._update(req)
    return result

##########################################################################################
  def storePilotOutput(self,pilotRef,output,error):
    """ Store standard output and error for a pilot with pilotRef
    """

    result = self._escapeString(output)
    if not result['OK']:
      return S_ERROR('Failed to escape output string')
    e_output = result['Value']
    result = self._escapeString(error)
    if not result['OK']:
      return S_ERROR('Failed to escape error string')
    e_error = result['Value']
    req = "UPDATE PilotAgents SET StdOutput='%s', StdError='%s' WHERE PilotJobReference='%s'"
    req = req % (e_output,e_error,pilotRef)
    result = self._update(req)
    return result

 ##########################################################################################
  def getPilotOutput(self,pilotRef):
    """ Retrieve standard output and error for pilot with pilotRef
    """

    req = "SELECT StdOutput, StdError FROM PilotAgents WHERE PilotJobReference='%s'" % pilotRef
    result = self._query(req)
    if not result['OK']:
      return result
    else:
      if result['Value']:
        stdout = result['Value'][0][0]
        error = result['Value'][0][0]
        if stdout == '""':
          stdout = ''
        if error == '""':
          error = ''
        return S_OK({'StdOut':stdout,'StdError':error})
      else:
        return S_ERROR('PilotJobReference '+pilotRef+' not found')

##########################################################################################
  def __getPilotID(self,pilotRef):
    """ Get Pilot ID for the given pilot reference
    """

    req = "SELECT PilotID from PilotAgents WHERE PilotJobReference='%s'" % pilotRef
    result = self._query(req)
    if not result['OK']:
      return 0
    else:
      if result['Value']:
        return int(result['Value'][0][0])
      else:
        return 0

##########################################################################################
  def setJobForPilot(self,jobID,pilotRef):
    """ Store the jobID of the job executed by the pilot with reference pilotRef
    """

    pilotID = self.__getPilotID(pilotRef)
    if pilotID:
      req = "INSERT INTO JobToPilotMapping VALUES (%d,%d,UTC_TIMESTAMP())" % (pilotID,jobID)
      result = self._update(req)
      return result
    else:
      return S_ERROR('PilotJobReference '+pilotRef+' not found')
      
##########################################################################################
  def setPilotCurrentJob(self,pilotRef,jobID):
    """ Set the pilot agent current DIRAC job ID
    """

    req = "UPDATE PilotAgents SET CurrentJobID=%d WHERE PilotJobReference='%s'" % (jobID,pilotRef)
    result = self._update(req)
    return result      

##########################################################################################
  def getExePilotsForJob(self,jobID):
    """ Get IDs of Pilot Agents that attempted to execute the given job
    """
    req = "SELECT PilotID FROM JobToPilotMapping WHERE JobID=%d ORDER BY StartTime" % jobID
    result = self._query(req)
    if not result['OK']:
      return result
    else:
      if result['Value']:
        pilotList = [ x[0] for x in result['Value'] ]
        return S_OK(pilotList)
      else:
        return S_ERROR('PilotJobReference '+pilotRef+' not found'  )

##########################################################################################
  def getPilotsForJob(self,jobID,gridType=None):
    """ Get IDs of Pilot Agents that were submitted for the given job, specify optionally the grid type
    """

    if gridType:
      req = "SELECT PilotJobReference FROM PilotAgents WHERE InitialJobID=%s AND GridType='%s' " % (jobID,gridType)
    else:
      req = "SELECT PilotJobReference FROM PilotAgents WHERE InitialJobID=%d " % jobID

    result = self._query(req)
    if not result['OK']:
      return result
    else:
      if result['Value']:
        pilotList = [ x[0] for x in result['Value'] ]
        return S_OK(pilotList)
      else:
        return S_ERROR('PilotJobReferences for job %d not found' % jobID)

##########################################################################################
  def getPilotCurrentJob(self,pilotRef):
    """ The the job ID currently executed by the pilot
    """
    req = "SELECT CurrentJobID FROM PilotAgents WHERE PilotJobReference='%s' " % pilotRef

    result = self._query(req)
    if not result['OK']:
      return result
    else:
      if result['Value']:
        jobID = int(result['Value'][0][0])
        return S_OK(jobID)
      else:
        return S_ERROR('Current job ID for pilot %s is not known' % pilotRef)

##########################################################################################
  def getPilotOwner(self,pilotRef):
    """ Get the pilot owner DN and group for the given pilot job reference
    """

    req = "SELECT OwnerDN, OwnerGroup FROM PilotAgents WHERE PilotJobReference='%s'" % pilotRef
    result = self._query(req)
    if not result['OK']:
      return result
    else:
      if result['Value']:
        ownerTuple = (result['Value'][0][0],result['Value'][0][1])
        return S_OK(ownerTuple)
      else:
        return S_ERROR('PilotID '+str(pilotID)+' not found')

##########################################################################################
  def getPilotSummary(self,startdate='',enddate=''):
    """ Get summary of the pilot jobs status by site
    """
    st_list = ['Aborted','Running','Done','Submitted','Ready','Scheduled','Waiting']

    summary_dict = {}
    summary_dict['Total'] = {}

    for st in st_list:
      summary_dict['Total'][st] = 0
      req = "SELECT DestinationSite,count(DestinationSite) FROM PilotAgents " + \
            "WHERE Status='%s' " % st
      if startdate:
        req = req + " AND SubmissionDate >= '%s'" % startdate
      if enddate:
        req = req + " AND SubmissionDate <= '%s'" % enddate

      req = req + " GROUP BY DestinationSite"
      result = self._query(req)
      if not result['OK']:
        return result
      else:
        if result['Value']:
          for res in result['Value']:
            site = res[0]
            count = res[1]
            if site:
              if not summary_dict.has_key(site):
                summary_dict[site] = {}
              summary_dict[site][st] = int(count)
              summary_dict['Total'][st] += int(count)

    return S_OK(summary_dict)
