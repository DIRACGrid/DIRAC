########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/DB/PilotAgentsDB.py,v 1.26 2008/07/17 19:12:55 acasajus Exp $
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

__RCSID__ = "$Id: PilotAgentsDB.py,v 1.26 2008/07/17 19:12:55 acasajus Exp $"

from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Base.DB import DB
from types import *
import threading

DEBUG = 1

#############################################################################
class PilotAgentsDB(DB):

  def __init__(self, maxQueueSize=10 ):

     DB.__init__(self,'PilotAgentsDB','WorkloadManagement/PilotAgentsDB',maxQueueSize)
     self.lock = threading.Lock()

##########################################################################################
  def addPilotReference(self,pilotRef,jobID,ownerDN,ownerGroup,broker='Unknown',
                        gridType='DIRAC',requirements='Unknown'):
    """ Add a new pilot job reference """

    result = self._getConnection()
    if result['OK']:
      connection = result['Value']
    else:
      return S_ERROR('Failed to get connection to MySQL: '+result['Message'])

    result = self._escapeString(requirements)
    if not result['OK']:
      gLogger.warn('Failed to escape requirements string')
      e_requirements = "Failed to escape requirements string"
    e_requirements = result['Value']

    parentID = 0

    for ref in pilotRef:

      req = "INSERT INTO PilotAgents( PilotJobReference, InitialJobID, OwnerDN, " + \
            "OwnerGroup, Broker, GridType, SubmissionTime, LastUpdateTime, Status, ParentID ) " + \
            "VALUES ('%s',%d,'%s','%s','%s','%s',UTC_TIMESTAMP(),UTC_TIMESTAMP(),'Submitted', %s)" % \
            (ref,int(jobID),ownerDN,ownerGroup,broker,gridType, parentID)

      result = self._update(req,connection)
      if not result['OK']:
        return result

      if not parentID:
        req = "SELECT LAST_INSERT_ID();"
        res = self._query(req,connection)
        if not res['OK']:
          return res
        parentID = int(res['Value'][0][0])

    req = "INSERT INTO PilotRequirements (PilotID,Requirements) VALUES (%d,'%s')" % (parentID,e_requirements)
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
  def selectPilots(self,statusList=[],owner=None,ownerGroup=None,newer=None,older=None):
    """ Select pilot references according to the provided criteria. "newer" and "older"
        specify the time interval in minutes
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
    if newer:
      condList.append("SubmissionTime > DATE_SUB(UTC_TIMESTAMP(),INTERVAL %d MINUTE)" % newer)
    if older:
      condList.append("SubmissionTime < DATE_SUB(UTC_TIMESTAMP(),INTERVAL %d MINUTE)" % older)

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
    result = self._update(req)
    if not result['OK']:
      return result


##########################################################################################
  def clearPilots(self,interval=30,aborted_interval=7):
    """ Delete all the pilot references submitted before <interval> days """

    req = "DELETE FROM PilotAgents WHERE SubmissionTime < DATE_SUB(CURDATE(),INTERVAL %d DAY)" % interval
    result = self._update(req)
    if not result['OK']:
      gLogger.warn('Error while clearing up pilots')
    req = "DELETE FROM PilotAgents WHERE Status='Aborted' AND"
    req += " SubmissionTime < DATE_SUB(CURDATE(),INTERVAL %d DAY)" % aborted_interval
    result = self._update(req)
    if not result['OK']:
      gLogger.warn('Error while clearing up aborted pilots')

    return S_OK()

##########################################################################################
  def getPilotInfo( self, pilotRef = False, parentId = False ):
    """ Get all the information for the pilot job reference or reference list
    """

    parameters = ['PilotJobReference','OwnerDN','OwnerGroup','GridType','Broker',
                  'Status','DestinationSite','BenchMark','ParentId','SubmissionTime', 'PilotID' ]

    expectList = False
    cmd = "SELECT %s FROM PilotAgents" % ", ".join( parameters )
    condSQL = []
    if pilotRef:
      if type( pilotRef ) == ListType:
        expectList = True
        condSQL.append( "PilotJobReference IN (%s)" % ",".join( [ '"%s"' % x for x in pilotRef ] ) )
      else:
        condSQL.append( "PilotJobReference = '%s'" % pilotRef )
    if parentId:
      if type( parentId ) == ListType:
        expectList = True
        condSQL.append( "ParentID IN (%s)" % ",".join( [ '"%s"' % x for x in parentId ] ) )
      else:
        condSQL.append( "ParentID = '%s'" % parentId )
    if condSQL:
      cmd = "%s WHERE %s" % ( cmd, " AND ".join( condSQL ) )

    result = self._query( cmd )
    if not result['OK']:
      return result

    if not result['Value']:
      msg = "No pilots found"
      if pilotRef:
        msg += " for PilotJobReference(s): %s" % pilotRef
      if parentId:
        msg += " with parent id: %s" % parentId
      return S_ERROR( msg )

    resDict = {}
    for row in result['Value']:
      pilotDict = {}
      for i in range(len(parameters)-1):
        pilotDict[parameters[i+1]] = row[i+1]
      resDict[row[0]] = pilotDict

    if expectList:
      return S_OK( resDict )
    else:
      if resDict:
        return S_OK( resDict[resDict.keys()[0]])
      else:
        return S_ERROR( "No pilots found" )



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

    pilotID = self.__getPilotID(pilotRef)
    if not pilotID:
      return S_ERROR('Pilot reference not found %s' % pilotRef)

    result = self._escapeString(requirements)
    if not result['OK']:
      return S_ERROR('Failed to escape requirements string')
    e_requirements = result['Value']
    req = "UPDATE PilotRequirements SET Requirements='%s' WHERE PilotID=%d" % (e_requirements,pilotID)
    result = self._update(req)
    return result

##########################################################################################
  def storePilotOutput(self,pilotRef,output,error):
    """ Store standard output and error for a pilot with pilotRef
    """

    pilotID = self.__getPilotID(pilotRef)
    if not pilotID:
      return S_ERROR('Pilot reference not found %s' % pilotRef)

    result = self._escapeString(output)
    if not result['OK']:
      return S_ERROR('Failed to escape output string')
    e_output = result['Value']
    result = self._escapeString(error)
    if not result['OK']:
      return S_ERROR('Failed to escape error string')
    e_error = result['Value']
    req = "INSERT INTO PilotOutput VALUES (%d,'%s','%s')" % (pilotID,e_output,e_error)
    result = self._update(req)
    return result

##########################################################################################
  def getPilotOutput(self,pilotRef):
    """ Retrieve standard output and error for pilot with pilotRef
    """

    req = "SELECT StdOutput, StdError FROM PilotOutput,PilotAgents WHERE "
    req += "PilotOutput.PilotID = PilotAgents.PilotID AND PilotAgents.PilotJobReference='%s'" % pilotRef
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
  def setCurrentJobID(self,pilotRef,jobID):
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
  def getJobsForPilot( self, pilotID ):
    """ Get IDs of Jobs that were executed by a pilot
    """
    req = "SELECT JobID FROM JobToPilotMapping WHERE pilotID=%d" % pilotID
    result = self._query(req)
    if not result['OK']:
      return result
    else:
      if result['Value']:
        pilotList = [ x[0] for x in result['Value'] ]
        return S_OK(pilotList)
      else:
        return S_ERROR('JobID '+pilotRef+' not found'  )

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

