########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/DB/PilotAgentsDB.py,v 1.4 2008/01/13 21:53:08 paterson Exp $
########################################################################
""" PilotAgentsDB class is a front-end to the Pilot Agent Database.
    This database keeps track of all the submitted grid pilot jobs.
    It also registers the mapping of the DIRAC jobs to the pilot
    agents.

    Available methods are:

    addPilotReference()

"""

__RCSID__ = "$Id: PilotAgentsDB.py,v 1.4 2008/01/13 21:53:08 paterson Exp $"

from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Base.DB import DB

# Here for debugging purpose; should be initialized by the containing component
gLogger.initialize('WMS','/Databases/PilotAgentsDB/Test')

#############################################################################
class PilotAgentsDB(DB):

  def __init__(self, maxQueueSize=10 ):
     DB.__init__(self,'PilotAgentsDB','WorkloadManagement/PilotAgentsDB',maxQueueSize)

##########################################################################################
  def addPilotReference(self,pilotRef,ownerDN,jobID,ownerGroup='',pilotType='DIRAC'):
    """ Add a new pilot job reference """

    req = "INSERT INTO PilotAgents( PilotJobReference, InitialJobID, OwnerDN, " + \
          "OwnerGroup, WMSType, SubmissionTime, LastUpdateTime, Status ) " + \
          "VALUES ('%s',%d,'%s',%s,'%s',NOW(),NOW(),'Submitted')" % \
          (pilotRef,int(jobID),ownerDN,ownerGroup,pilotType)
    return self._update(req)

##########################################################################################
  def setPilotStatus(self,pilotRef,status,destination=None):
    """ Set pilot job LCG status """

    if not destination:
      req = "UPDATE PilotAgents set Status='%s',LastUpdateTime=NOW() " + \
            "WHERE PilotJobReference='%s'" % (status,pilotRef)
    else:
      req = "UPDATE LCGPilots set Status='%s',LastUpdate=NOW(), DestinationSite='%s' " + \
            "WHERE PilotJobReference='%s'" % (status,destination,pilotRef)

    return self._update(req)

##########################################################################################
  def deletePilot(self,pilotRef):
    """ Delete Pilot reference from the LCGPilots table """

    req = "DELETE FROM LCGPilots WHERE LCGJobReference='%s'" % pilotRef
    return self._update(req)

##########################################################################################
  def clearPilots(self,interval=20):
    """ Delete all the pilot references submitted before <interval> days """

    req = "DELETE FROM LCGPilots WHERE SubmissionDate < DATE_SUB(CURDATE(),INTERVAL %d DAY)" % interval
    return self._update(req)

##########################################################################################
  def getLCGPilotOwnerDN(self,pilotRef):
    """ Get an OwnerDN for the LCG pilot reference """

    req = "SELECT OwnerDN FROM LCGPilots WHERE LCGJobReference='%s'" % pilotRef
    result = self._query(req)
    if not result['OK']:
      return result
    else:
      if result['Value']:
        return S_OK(result['Value'][0][0])
      else:
        return S_ERROR('LCGJobReference '+pilotRef+' not found')

##########################################################################################

  def getLCGPilotOwnerDNForJob(self,jobID):
    """ Get an OwnerDN for the LCG pilot for the given Dirac JobID """

    result = self.getJobParameters(jobID,['EDG_WL_JOBID'])
    if result['OK']:
      if result['Value']['EDG_WL_JOBID'] != "Unknown":
        pilotRef = result['Value']['EDG_WL_JOBID']
        result = self.getLCGPilotOwnerDN(pilotRef)
        if result['OK']:
          if result['Value']:
            res = S_OK(result['Value'])
            res['LCGJobReference'] = pilotRef
            return res
          else:
            return S_ERROR('getLCGPilotOwnerDNForJob: no pilot reference found')
        else:
          return S_ERROR('getLCGPilotOwnerDNForJob: no pilot reference found')
      else:
        return S_ERROR('getLCGPilotOwnerDNForJob: job does not have pilot reference')
    else:
      return S_ERROR('getLCGPilotOwnerDNForJob: failed to get job parameters')

##########################################################################################

  def getLCGPilotSummary(self,startdate='',enddate=''):
    """ Get summary of the pilot jobs status by site
    """
    st_list = ['Aborted','Running','Done','Submitted','Ready','Scheduled','Waiting']

    summary_dict = {}

    for st in st_list:
      req = "SELECT Destination,count(Destination) FROM LCGPilots " + \
            "WHERE Status='%s' " % st
      if startdate:
        req = req + " AND SubmissionDate >= '%s'" % startdate
      if enddate:
        req = req + " AND SubmissionDate <= '%s'" % enddate

      req = req + " GROUP BY Destination"
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
