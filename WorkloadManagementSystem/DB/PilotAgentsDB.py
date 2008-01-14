########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/DB/PilotAgentsDB.py,v 1.5 2008/01/14 22:10:42 atsareg Exp $
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

__RCSID__ = "$Id: PilotAgentsDB.py,v 1.5 2008/01/14 22:10:42 atsareg Exp $"

from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Base.DB import DB

DEBUG = 1

#############################################################################
class PilotAgentsDB(DB):

  def __init__(self, maxQueueSize=10 ):
     DB.__init__(self,'PilotAgentsDB','WorkloadManagement/PilotAgentsDB',maxQueueSize)

##########################################################################################
  def addPilotReference(self,pilotRef,jobID,ownerDN,ownerGroup,gridType='DIRAC'):
    """ Add a new pilot job reference """

    req = "INSERT INTO PilotAgents( PilotJobReference, InitialJobID, OwnerDN, " + \
          "OwnerGroup, GridType, SubmissionTime, LastUpdateTime, Status ) " + \
          "VALUES ('%s',%d,'%s',%s,'%s',NOW(),NOW(),'Submitted')" % \
          (pilotRef,int(jobID),ownerDN,ownerGroup,gridType)
    return self._update(req)

##########################################################################################
  def setPilotStatus(self,pilotRef,status,destination=None):
    """ Set pilot job LCG status """

    if not destination:
      req = "UPDATE PilotAgents SET Status='%s',LastUpdateTime=NOW() " + \
            "WHERE PilotJobReference='%s'" % (status,pilotRef)
    else:
      req = "UPDATE LCGPilots SET Status='%s',LastUpdate=NOW(), DestinationSite='%s' " + \
            "WHERE PilotJobReference='%s'" % (status,destination,pilotRef)

    return self._update(req)

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
  def getPilotOwner(self,pilotRef):
    """ Get an OwnerDN for the LCG pilot reference """

    req = "SELECT OwnerDN,OwnerGroup FROM PilotAgents WHERE PilotJobReference='%s'" % pilotRef
    result = self._query(req)
    if not result['OK']:
      return result
    else:
      if result['Value']:
        return S_OK((result['Value'][0][0],result['Value'][0][1]))
      else:
        return S_ERROR('PilotJobReference '+pilotRef+' not found')

##########################################################################################
  def setPilotDestinationSite(self,pilotRef,destination):
    """ Set the pilot agent destination site
    """

    req = "UPDATE PilotAgents SET DestinationSite='%s' WHERE PilotJobReference='%s'" % pilotRef
    result = self._update(req)
    return result

##########################################################################################
  def storePilotOutput(self,pilotRef,output,error):
    """ Store standard output and error for a pilot with pilotRef
    """

    req = "UPDATE PilotAgents SET StdOutput='%s', StdError='%s' WHERE PilotJobReference='%s'" % pilotRef
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
        return S_OK((result['Value'][0][0],result['Value'][0][1]))
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
      req = "INSERT INTO JobToPilotMapping VALUES (%d,%d,NOW())" % (pilotID,jobID)
      result = self._update(req)
      return result
    else:
      return S_ERROR('PilotJobReference '+pilotRef+' not found')

##########################################################################################
  def getPilotsForJob(self,jobID):
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
        return S_ERROR('PilotJobReference '+pilotRef+' not found')

##########################################################################################
  def getPilotOwner(self,pilotID):
    """ Get the pilot owner DN and group for the given pilotID
    """

    req = "SELECT OwnerDN, OwnerGroup FROM PilotAgents WHERE PilotID=%d" % pilotID
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

  def getPilotsSummary(self,startdate='',enddate=''):
    """ Get summary of the pilot jobs status by site
    """
    st_list = ['Aborted','Running','Done','Submitted','Ready','Scheduled','Waiting']

    summary_dict = {}

    for st in st_list:
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

    return S_OK(summary_dict)
e