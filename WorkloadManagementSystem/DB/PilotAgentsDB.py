########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/DB/PilotAgentsDB.py,v 1.45 2008/12/22 10:45:50 rgracian Exp $
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

__RCSID__ = "$Id: PilotAgentsDB.py,v 1.45 2008/12/22 10:45:50 rgracian Exp $"

from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Base.DB import DB
from DIRAC.Core.Utilities.SiteCEMapping import getSiteForCE, getCESiteMapping
import DIRAC.Core.Utilities.Time as Time
from types import *
import threading, datetime, time

DEBUG = 1

#############################################################################
class PilotAgentsDB(DB):

  def __init__(self, maxQueueSize=10 ):

     DB.__init__(self,'PilotAgentsDB','WorkloadManagement/PilotAgentsDB',maxQueueSize)
     self.lock = threading.Lock()

##########################################################################################
  def addPilotReference(self,pilotRef,jobID,ownerDN,ownerGroup,broker='Unknown',
                        gridType='DIRAC',requirements='Unknown',taskQueueID=0):
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

    parentID = -1
    if len(pilotRef) > 1:
      parentID = 0

    for ref in pilotRef:

      req = "INSERT INTO PilotAgents( PilotJobReference, InitialJobID, TaskQueueID, OwnerDN, " + \
            "OwnerGroup, Broker, GridType, SubmissionTime, LastUpdateTime, Status, ParentID ) " + \
            "VALUES ('%s',%d,%d,'%s','%s','%s','%s',UTC_TIMESTAMP(),UTC_TIMESTAMP(),'Submitted', %s)" % \
            (ref,int(jobID),int(taskQueueID),ownerDN,ownerGroup,broker,gridType, parentID)

      result = self._update(req,connection)
      if not result['OK']:
        connection.close()
        return result

      if parentID < 1 :
        req = "SELECT LAST_INSERT_ID();"
        res = self._query(req,connection)
        if not res['OK']:
          connection.close()
          return res
        parentID = int(res['Value'][0][0])

    connection.close()

    req = "INSERT INTO PilotRequirements (PilotID,Requirements) VALUES (%d,'%s')" % (parentID,e_requirements)
    return self._update(req)

##########################################################################################
  def addPilotTQReference(self,pilotRef,taskQueueID,ownerDN,ownerGroup,broker='Unknown',
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

    for ref in pilotRef:

      req = "INSERT INTO PilotAgents( PilotJobReference, TaskQueueID, OwnerDN, " + \
            "OwnerGroup, Broker, GridType, SubmissionTime, LastUpdateTime, Status ) " + \
            "VALUES ('%s',%d,'%s','%s','%s','%s',UTC_TIMESTAMP(),UTC_TIMESTAMP(),'Submitted')" % \
            (ref,int(taskQueueID),ownerDN,ownerGroup,broker,gridType)

      result = self._update(req,connection)
      if not result['OK']:
        connection.close()
        return result

      req = "SELECT LAST_INSERT_ID();"
      res = self._query(req,connection)
      if not res['OK']:
        connection.close()
        return res
      pilotID = int(res['Value'][0][0])

      req = "INSERT INTO PilotRequirements (PilotID,Requirements) VALUES (%d,'%s')" % (pilotID,e_requirements)
      res = self._update(req,connection)
      if not res['OK']:
        connection.close()
        return res

    connection.close()

    return S_OK()

##########################################################################################
  def setPilotStatus( self, pilotRef, status, destination=None, updateTime=None, conn = False ):
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

    return self._update( req, conn = conn )

##########################################################################################
  def selectPilots(self,condDict, older=None, newer=None, timeStamp='SubmissionTime',
                        orderAttribute=None, limit=None):
    """ Select pilot references according to the provided criteria. "newer" and "older"
        specify the time interval in minutes
    """

    condition = self.buildCondition(condDict, older, newer, timeStamp)
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

    req = "SELECT PilotJobReference from PilotAgents"
    if condition:
      req += " %s " % condition
    result = self._query(req)
    if not result['OK']:
      return result

    pilotList = []
    if result['Value']:
      pilotList = [ x[0] for x in result['Value']]

    return S_OK(pilotList)


##########################################################################################
  def countPilots(self,condDict, older=None, newer=None, timeStamp='SubmissionTime' ):
    """ Select pilot references according to the provided criteria. "newer" and "older"
        specify the time interval in minutes
    """

    condition = self.buildCondition(condDict, older, newer, timeStamp)

    req = "SELECT COUNT(PilotID) from PilotAgents"
    if condition:
      req += " %s " % condition
    result = self._query(req)
    if not result['OK']:
      return result

    return S_OK(result['Value'][0][0])


##########################################################################################
  def getPilotGroups( self, groupList=['Status', 'OwnerDN', 'OwnerGroup', 'GridType'], condDict={} ):
    """
     Get all exisiting combinations of groupList Values
    """

    cmd = 'SELECT %s from PilotAgents ' % ', '.join(groupList)

    condList= []
    for cond in condDict:
      condList.append('%s in ( "%s" )' % ( cond, '", "'.join([ str(y) for y in condDict[cond]]  ) ) )

    if condList:
      cmd += ' WHERE %s ' % ' AND '.join(condList)

    cmd += ' GROUP BY %s' % ', '.join(groupList)

    return self._query(cmd)


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
  def getPilotInfo( self, pilotRef = False, parentId = False, conn = False, paramNames = [], pilotID = False ):
    """ Get all the information for the pilot job reference or reference list
    """

    parameters = ['PilotJobReference','OwnerDN','OwnerGroup','GridType','Broker',
                  'Status','DestinationSite','BenchMark','ParentID',
                  'SubmissionTime', 'PilotID', 'LastUpdateTime' ]
    if paramNames:
      parameters = paramNames

    cmd = "SELECT %s FROM PilotAgents" % ", ".join( parameters )
    condSQL = []
    if pilotRef:
      if type( pilotRef ) == ListType:
        condSQL.append( "PilotJobReference IN (%s)" % ",".join( [ '"%s"' % x for x in pilotRef ] ) )
      else:
        condSQL.append( "PilotJobReference = '%s'" % pilotRef )
    if pilotID:
      if type( pilotID ) == ListType:
        condSQL.append( "PilotID IN (%s)" % ",".join( [ '%s' % x for x in pilotID ] ) )
      else:
        condSQL.append( "PilotID = '%s'" % pilotID )
    if parentId:
      if type( parentId ) == ListType:
        condSQL.append( "ParentID IN (%s)" % ",".join( [ '%s' % x for x in parentId ] ) )
      else:
        condSQL.append( "ParentID = %s" % parentId )
    if condSQL:
      cmd = "%s WHERE %s" % ( cmd, " AND ".join( condSQL ) )

    result = self._query( cmd, conn = conn )
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
    pilotIDs = []
    for row in result['Value']:
      pilotDict = {}
      for i in range( len( parameters ) ):
        pilotDict[ parameters[i] ] = row[ i ]
        if parameters[i] == 'PilotID':
          pilotIDs.append( row[i] )
      resDict[ row[0] ] = pilotDict

    result = self.getJobsForPilot( pilotIDs )
    if not result['OK']:
      return S_OK( resDict )

    jobsDict = result[ 'Value' ]
    for pilotRef in resDict:
      pilotInfo = resDict[ pilotRef ]
      pilotID = pilotInfo[ 'PilotID' ]
      if pilotID in jobsDict:
        pilotInfo[ 'Jobs' ] = jobsDict[ pilotID ]

    return S_OK( resDict )

##########################################################################################
  def setPilotDestinationSite(self,pilotRef,destination):
    """ Set the pilot agent destination site
    """

    gridSite = 'Unknown'
    result = getSiteForCE(destination)
    if result['OK']:
      gridSite = result['Value']

    req = "UPDATE PilotAgents SET DestinationSite='%s', GridSite='%s' WHERE PilotJobReference='%s'" % (destination,gridSite,pilotRef)
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
    cmd = "SELECT pilotID,JobID FROM JobToPilotMapping "
    if type( pilotID ) == ListType:
      cmd = cmd + " WHERE pilotID IN (%s)" % ",".join( [ '%s' % x for x in pilotID ] )
    else:
      cmd = cmd + " WHERE pilotID = %s" % pilotID

    result = self._query(cmd)
    if not result['OK']:
      return result

    resDict = {}
    for row in result['Value']:
      if not row[0] in resDict:
        resDict[ row[0] ] = []
      resDict[ row[0] ].append( row[1] )
    return S_OK(resDict)

##########################################################################################
  def getPilotsForJob(self,jobID,gridType=None):
    """ Get IDs of Pilot Agents that were submitted for the given job, specify optionally the grid type
    """

    if gridType:
      req = "SELECT PilotJobReference FROM PilotAgents WHERE InitialJobID=%s AND GridType='%s' " % (jobID,gridType)
    else:
      req = "SELECT PilotJobReference FROM PilotAgents WHERE InitialJobID=%s " % jobID

    result = self._query(req)
    if not result['OK']:
      return result
    else:
      if result['Value']:
        pilotList = [ x[0] for x in result['Value'] ]
        return S_OK(pilotList)
      else:
        return S_ERROR('PilotJobReferences for job %s not found' % jobID)

##########################################################################################
  def getPilotsForTaskQueue(self,taskQueueID,gridType=None,limit=None):
    """ Get IDs of Pilot Agents that were submitted for the given taskQueue,
        specify optionally the grid type, results are sorted by Submission time
        an Optional limit can be set.
    """

    if gridType:
      req = "SELECT PilotID FROM PilotAgents WHERE TaskQueueID=%s AND GridType='%s' " % (taskQueueID,gridType)
    else:
      req = "SELECT PilotID FROM PilotAgents WHERE TaskQueueID=%s " % taskQueueID

    req += 'ORDER BY SubmissionTime '

    if limit:
      req += 'LIMIT %s' % limit

    result = self._query(req)
    if not result['OK']:
      return result
    else:
      if result['Value']:
        pilotList = [ x[0] for x in result['Value'] ]
        return S_OK(pilotList)
      else:
        return S_ERROR('PilotJobReferences for TaskQueueID %s not found' % taskQueueID)

##########################################################################################
  def getPilotsForJobID(self,jobID):
    """ Get ID of Pilot Agent that is running a given JobID
    """

    result = self._query( 'SELECT PilotID FROM JobToPilotMapping WHERE JobID=%s' % jobID )

    if not result['OK']:
      return result

    if result['Value']:
      pilotList = [ x[0] for x in result['Value'] ]
      return S_OK(pilotList)
    else:
      return S_ERROR('PilotID for job %d not found' % jobID)

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

    # Get aborted pilots in the last hour, day
    req = "SELECT DestinationSite,count(DestinationSite) FROM PilotAgents WHERE Status='Aborted' AND "
    reqDict = {}
    reqDict['Aborted_Hour'] = req + " LastUpdateTime >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 1 HOUR)"
    reqDict['Aborted_Day'] = req + " LastUpdateTime >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 1 DAY)"

    for key, req in reqDict.items():
      result = self._query(req)
      if not result['OK']:
        break
      if result['Value']:
        for res in result['Value']:
          site = res[0]
          count = res[1]
          if site:
            if summary_dict.has_key(site):
              summary_dict[site][key] = int(count)

    return S_OK(summary_dict)

##########################################################################################
  def getPilotSummaryWeb(self,selectDict,sortList,startItem,maxItems):
    """ Get summary of the pilot jobs status by CE/site in a standard structure
    """

    stateNames = ['Submitted','Ready','Scheduled','Waiting','Running','Done','Aborted']
    allStateNames = stateNames + ['Done_Empty','Aborted_Hour']
    paramNames = ['Site','CE']+allStateNames

    resultDict = {}
    last_update = None
    if selectDict.has_key('LastUpdateTime'):
      last_update = selectDict['LastUpdateTime']
      del selectDict['LastUpdateTime']

    start = time.time()
    # Get all the data from the database with various selections
    result = self.getCounters('PilotAgents',
                              ['GridSite','DestinationSite','Status'],
                              selectDict,newer=last_update,timeStamp='LastUpdateTime')
    if not result['OK']:
      return result

    last_update = Time.dateTime() - Time.hour
    selectDict['Status'] = 'Aborted'
    resultHour = self.getCounters('PilotAgents',
                                 ['GridSite','DestinationSite','Status'],
                                 selectDict,newer=last_update,timeStamp='LastUpdateTime')
    if not resultHour['OK']:
      return resultHour

    last_update = Time.dateTime() - Time.day
    selectDict['Status'] = ['Aborted','Done']
    resultDay = self.getCounters('PilotAgents',
                                 ['GridSite','DestinationSite','Status'],
                                 selectDict,newer=last_update,timeStamp='LastUpdateTime')
    if not resultDay['OK']:
      return resultDay
    selectDict['CurrentJobID'] = 0
    selectDict['Status'] = 'Done'
    resultDayEmpty = self.getCounters('PilotAgents',
                                 ['GridSite','DestinationSite','Status'],
                                 selectDict,newer=last_update,timeStamp='LastUpdateTime')
    if not resultDayEmpty['OK']:
      return resultDayEmpty

    ceMap = {}
    resMap = getCESiteMapping()
    if resMap['OK']:
      ceMap = resMap['Value']

    # Sort out different counters
    resultDict = {}
    resultDict['Unknown']={}
    for attDict,count in result['Value']:
      site = attDict['GridSite']
      ce = attDict['DestinationSite']
      state = attDict['Status']
      if site == 'Unknown' and ce != "Unknown" and ce != "Multiple" and ceMap.has_key(ce):
        site = ceMap[ce]
      if not resultDict.has_key(site):
        resultDict[site] = {}
      if not resultDict[site].has_key(ce):
        resultDict[site][ce] = {}
        for p in allStateNames:
          resultDict[site][ce][p] = 0

      resultDict[site][ce][state] = count

    for attDict,count in resultDay['Value']:
      site = attDict['GridSite']
      ce = attDict['DestinationSite']
      state = attDict['Status']
      if site == 'Unknown' and ce != "Unknown" and ceMap.has_key(ce):
        site = ceMap[ce]
      if state == "Done":
        resultDict[site][ce]["Done"] = count
      if state == "Aborted":
        resultDict[site][ce]["Aborted"] = count

    for attDict,count in resultDayEmpty['Value']:
      site = attDict['GridSite']
      ce = attDict['DestinationSite']
      state = attDict['Status']
      if site == 'Unknown' and ce != "Unknown" and ceMap.has_key(ce):
        site = ceMap[ce]
      if state == "Done":
        resultDict[site][ce]["Done_Empty"] = count

    for attDict,count in resultHour['Value']:
      site = attDict['GridSite']
      ce = attDict['DestinationSite']
      state = attDict['Status']
      if site == 'Unknown' and ce != "Unknown" and ceMap.has_key(ce):
        site = ceMap[ce]
      if state == "Aborted":
        resultDict[site][ce]["Aborted_Hour"] = count

    records = []
    siteSumDict = {}
    for site in resultDict:
      sumDict = {}
      sumDict['Total'] = 0
      for ce in resultDict[site]:
        itemList = [site,ce]
        total = 0
        for state in allStateNames:
          if not sumDict.has_key(state):
            sumDict[state] = 0
          itemList.append(resultDict[site][ce][state])
          sumDict[state] += resultDict[site][ce][state]
          if state == "Done":
            done = resultDict[site][ce][state]
          if state == "Done_Empty":
            empty = resultDict[site][ce][state]
          if state == "Aborted":
            aborted = resultDict[site][ce][state]
          if state == "Aborted_Hour":
            aborted_hour = resultDict[site][ce][state]
          if state != "Aborted_Hour" and state != "Done_Empty":
            total += resultDict[site][ce][state]

        sumDict['Total'] += total
        # Add the total number of pilots seen in the last day
        itemList.append(total)
        # Add pilot submission efficiency evaluation
        if done > 0:
          eff = float(done-empty)/float(done)*100.
        else:
          eff = 100.
        itemList.append('%.2f' % eff)
        # Add pilot job efficiency evaluation
        if total > 0:
          eff = float(total-aborted)/float(total)*100.
        else:
          eff = 100.
        itemList.append('%.2f' % eff)

        # Evaluate the quality status of the CE
        if total > 10:
          if eff < 25.:
            itemList.append('Bad')
          elif eff < 60.:
            itemList.append('Poor')
          elif eff < 85.:
            itemList.append('Fair')
          else:
            itemList.append('Good')
        else:
          itemList.append('Idle')
        records.append(itemList)

      itemList = [site,'All']
      for state in allStateNames+['Total']:
        itemList.append(sumDict[state])
      done = sumDict["Done"]
      empty = sumDict["Done_Empty"]
      aborted = sumDict["Aborted"]
      aborted_hour = sumDict["Aborted_Hour"]
      total = sumDict["Total"]

      # Add pilot submission efficiency evaluation
      if done > 0:
        eff = float(done-empty)/float(done)*100.
      else:
        eff = 100.
      itemList.append('%.2f' % eff)
      # Add pilot job efficiency evaluation
      if total > 0:
        eff = float(total-aborted)/float(total)*100.
      else:
        eff = 100.
      itemList.append('%.2f' % eff)

      # Evaluate the quality status of the Site
      if total > 10:
        if eff < 25.:
          itemList.append('Bad')
        elif eff < 60.:
          itemList.append('Poor')
        elif eff < 85.:
          itemList.append('Fair')
        else:
          itemList.append('Good')
      else:
        itemList.append('Idle')
      records.append(itemList)
      for state in allStateNames+['Total']:
        if not siteSumDict.has_key(state):
          siteSumDict[state] = sumDict[state]
        else:
          siteSumDict[state] += sumDict[state]

    finalDict = {}
    finalDict['TotalRecords'] = len(records)
    finalDict['ParameterNames'] = paramNames+ \
                                 ['Total','SubmissionEff','PilotJobEff','Status']

    # Return all the records if maxItems == 0 or the specified number otherwise
    if maxItems:
      finalDict['Records'] = records[startItem:startItem+maxItems]
    else:
      finalDict['Records'] = records

    done = siteSumDict["Done"]
    empty = siteSumDict["Done_Empty"]
    aborted = siteSumDict["Aborted"]
    aborted_hour = siteSumDict["Aborted_Hour"]
    total = siteSumDict["Total"]

    # Add pilot submission efficiency evaluation
    if done > 0:
      eff = float(done-empty)/float(done)*100.
    else:
      eff = 100.
    siteSumDict['SubmissionEff'] = '%.2f' % eff
    # Add pilot job efficiency evaluation
    if total > 0:
      eff = float(total-aborted)/float(total)*100.
    else:
      eff = 100.
    siteSumDict['PilotJobEff'] = '%.2f' % eff

    # Evaluate the overall quality status
    if total > 100:
      if eff < 25.:
        siteSumDict['Status'] = 'Bad'
      elif eff < 60.:
        siteSumDict['Status'] = 'Poor'
      elif eff < 85.:
        siteSumDict['Status'] = 'Fair'
      else:
        siteSumDict['Status'] = 'Good'
    else:
      siteSumDict['Status'] = 'Idle'
    finalDict['Extras'] = siteSumDict

    return S_OK(finalDict)

##########################################################################################
  def getPilotMonitorWeb(self,selectDict,sortList,startItem,maxItems):
    """ Get summary of the pilot job information in a standard structure
    """

    resultDict = {}
    last_update = None
    if selectDict.has_key('LastUpdateTime'):
      last_update = selectDict['LastUpdateTime']
      del selectDict['LastUpdateTime']

    # Sorting instructions. Only one for the moment.
    if sortList:
      orderAttribute = sortList[0][0]+":"+sortList[0][1]
    else:
      orderAttribute = None

    # Select pilots for the summary
    result = self.selectPilots(selectDict, orderAttribute=orderAttribute, newer=last_update)
    if not result['OK']:
      return S_ERROR('Failed to select pilots: '+result['Message'])

    pList = result['Value']
    nPilots = len(pList)
    resultDict['TotalRecords'] = nPilots
    if nPilots == 0:
      return S_OK(resultDict)

    ini = startItem
    last = ini + maxItems
    if ini >= nPilots:
      return S_ERROR('Item number out of range')
    if last > nPilots:
      last = nPilots
    pilotList = pList[ini:last]

    paramNames = ['PilotJobReference','OwnerDN','OwnerGroup','GridType','Broker',
                  'Status','DestinationSite','BenchMark','ParentID',
                  'SubmissionTime', 'PilotID', 'LastUpdateTime','CurrentJobID','TaskQueueID',
                  'GridSite']

    result = self.getPilotInfo(pilotList,paramNames=paramNames)
    if not result['OK']:
      return S_ERROR('Failed to get pilot info: '+result['Message'])

    pilotDict = result['Value']
    records = []
    for pilot in pilotList:
      parList = []
      for parameter in paramNames:
        if type(pilotDict[pilot][parameter]) not in [IntType,LongType]:
          parList.append(str(pilotDict[pilot][parameter]))
        else:
          parList.append(pilotDict[pilot][parameter])
        if parameter=='GridSite':
          gridSite = pilotDict[pilot][parameter]

      # If the Grid Site is unknown try to recover it in the last moment
      if gridSite == "Unknown":
        ce = pilotDict[pilot]['DestinationSite']
        result = getSiteForCE(ce)
        if result['OK']:
          gridSite = result['Value']
          del parList[-1]
          parList.append(gridSite)
      records.append(parList)

    resultDict['ParameterNames'] = paramNames
    resultDict['Records'] = records

    return S_OK(resultDict)
