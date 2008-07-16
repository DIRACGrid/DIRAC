########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/Agent/JobHistoryAgent.py,v 1.6 2008/07/16 16:46:13 acasajus Exp $


"""  JobHistoryAgent sends periodically numbers of jobs in various states for various
     sites to the Monitoring system to create historical plots.
"""

from DIRAC  import gLogger, gConfig, gMonitor,S_OK, S_ERROR
from DIRAC.Core.Base.Agent import Agent
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
from DIRAC.AccountingSystem.Client.Types.WMSHistory import WMSHistory
from DIRAC.AccountingSystem.Client.DataStoreClient import DataStoreClient
from DIRAC.Core.Utilities import Time

import time,os

AGENT_NAME = 'WorkloadManagement/JobHistoryAgent'
MONITOR_SITES = ['LCG.CERN.ch','LCG.IN2P3.fr','LCG.RAL.uk','LCG.CNAF.it',
                 'LCG.GRIDKA.de','LCG.NIKHEF.nl','LCG.PIC.es','All sites']
MONITOR_STATUS = ['Running','Stalled','Done','Failed']

class JobHistoryAgent(Agent):

  __summaryKeyFieldsMapping = [ 'Status',
                                'MinorStatus',
                                'ApplicationStatus',
                                'Site',
                                'User',
                                'UserGroup',
                                'JobGroup',
                                'JobSplitType',
                              ]
  __summaryValueFieldsMapping = [ 'Jobs',
                                  'Reschedules',
                                ]

  def __init__(self):
    """ Standard constructor
    """
    Agent.__init__( self, AGENT_NAME, initializeMonitor = True )

  def initialize(self):
    result = Agent.initialize(self)
    self.jobDB = JobDB()

    for status in MONITOR_STATUS:
      for site in MONITOR_SITES:
        gLogger.verbose("Registering activity %s-%s" % (status,site))
        gLogger.verbose("Jobs in %s state at %s" % (status,site))
        gMonitor.registerActivity("%s-%s" % (status,site),"Jobs in %s state at %s" % (status,site),
                                  "JobHistoryAgent","Jobs/minute",gMonitor.OP_MEAN)

    self.last_update = 0
    self.resultDB = None
    self.reportPeriod = 300
    return S_OK()

  def execute(self):
    """ Main execution method
    """

    delta = time.time() - self.last_update
    if delta > self.reportPeriod:
      result = self.jobDB.getCounters(['Status','Site'],{},'')
      if not result['OK']:
        return S_ERROR('Failed to get data from the Job Database')
      self.resultDB = result['Value']
      self.sendAccountingRecords()
      self.last_update = time.time()

    totalDict = {}
    for status in MONITOR_STATUS:
      totalDict[status] = 0

    for row in self.resultDB:
      dict = row[0]
      site = dict['Site']
      status = dict['Status']
      count = row[1]
      if site in MONITOR_SITES and status in MONITOR_STATUS:
        gLogger.verbose("Adding mark %s-%s: " % (status,site)+str(count))
        gMonitor.addMark("%s-%s" % (status,site),count)
        totalDict[status] += count

    for status in MONITOR_STATUS:
      gLogger.verbose("Adding mark %s-All sites: " % status + str(totalDict[status]))
      gMonitor.addMark("%s-All sites" % status,totalDict[status])

    return S_OK()


  def sendAccountingRecords(self):
    #Get the WMS Snapshot!
    result = self.jobDB.getSummarySnapshot()
    dsClients = {}
    now = Time.dateTime()
    if not result[ 'OK' ]:
      gLogger.error( "Can't the the jobdb summary", result[ 'Message' ] )
    else:
      fields = list( result[ 'Value' ][0] )
      values = result[ 'Value' ][1]
      for record in values:
        recordSetup = record[0]
        if recordSetup not in dsClients:
          dsClients[ recordSetup ] = DataStoreClient( setup = recordSetup )
        record = record[1:]
        rD = {}
        for iP in range( len( self.__summaryKeyFieldsMapping ) ):
          fieldName = self.__summaryKeyFieldsMapping[iP]
          rD[ fieldName ] = record[iP]
          if not rD[ fieldName ]:
            rD[ fieldName ] = 'unset'
        record = record[len( self.__summaryKeyFieldsMapping ):]
        for iP in range( len( self.__summaryValueFieldsMapping ) ):
          rD[ self.__summaryValueFieldsMapping[iP] ] = int( record[iP] )
        acWMS = WMSHistory()
        acWMS.setStartTime( now )
        acWMS.setEndTime( now )
        acWMS.setValuesFromDict( rD )
        retVal =  acWMS.checkValues()
        if not retVal[ 'OK' ]:
          print retVal[ 'Message' ]
          print rD
        dsClients[ recordSetup ].addRegister( acWMS )
      for setup in dsClients:
        result = dsClients[ setup ].commit()
        if not result[ 'OK' ]:
          gLogger.error( "Couldn't commit wms history for setup %s"  % setup, result[ 'Message' ] )






