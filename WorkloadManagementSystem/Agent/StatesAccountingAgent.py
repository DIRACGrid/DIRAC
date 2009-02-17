########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/Agent/StatesAccountingAgent.py,v 1.5 2009/02/17 16:25:26 acasajus Exp $

__RCSID__ = "$Id: StatesAccountingAgent.py,v 1.5 2009/02/17 16:25:26 acasajus Exp $"

"""  JobHistoryAgent sends periodically numbers of jobs in various states for various
     sites to the Monitoring system to create historical plots.
"""

from DIRAC  import gLogger, gConfig, gMonitor,S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
from DIRAC.AccountingSystem.Client.Types.WMSHistory import WMSHistory
from DIRAC.AccountingSystem.Client.DataStoreClient import DataStoreClient
from DIRAC.Core.Utilities import Time

import time,os

AGENT_NAME = 'WorkloadManagement/StatesAccountingAgent'

class StatesAccountingAgent(AgentModule):

  __summaryKeyFieldsMapping = [ 'Status',
                                'MinorStatus',
                                'Site',
                                'User',
                                'UserGroup',
                                'JobGroup',
                                'JobSplitType',
                              ]
  __summaryDefinedFields = [ ( 'ApplicationStatus', 'unset' ) ]
  __summaryValueFieldsMapping = [ 'Jobs',
                                  'Reschedules',
                                ]


  def initialize(self):
    """ Standard constructor
    """
    self.dsClients = {}
    self.jobDB = JobDB()

    self.reportPeriod = 300
    self.am_setOption( "PollingTime", self.reportPeriod )
    return S_OK()

  def execute(self):
    """ Main execution method
    """
    #Get the WMS Snapshot!
    result = self.jobDB.getSummarySnapshot()
    now = Time.dateTime()
    if not result[ 'OK' ]:
      gLogger.error( "Can't the the jobdb summary", result[ 'Message' ] )
    else:
      fields = list( result[ 'Value' ][0] )
      values = result[ 'Value' ][1]
      for record in values:
        recordSetup = record[0]
        if recordSetup not in self.dsClients:
          gLogger.info( "Creating DataStore client for %s" % recordSetup )
          self.dsClients[ recordSetup ] = DataStoreClient( setup = recordSetup, retryGraceTime = 900 )
        record = record[1:]
        for FV in self.__summaryDefinedFields:
          rD = { FV[0] : FV[1] }
        for iP in range( len( self.__summaryKeyFieldsMapping ) ):
          fieldName = self.__summaryKeyFieldsMapping[iP]
          rD[ fieldName ] = record[iP]
        record = record[ len( self.__summaryKeyFieldsMapping ): ]
        for iP in range( len( self.__summaryValueFieldsMapping ) ):
          rD[ self.__summaryValueFieldsMapping[iP] ] = int( record[iP] )
        acWMS = WMSHistory()
        acWMS.setStartTime( now )
        acWMS.setEndTime( now )
        acWMS.setValuesFromDict( rD )
        retVal =  acWMS.checkValues()
        if not retVal[ 'OK' ]:
          gLogger.error( "Invalid accounting record ", "%s -> %s" % ( retVal[ 'Message' ], rD ) )
        else:
          self.dsClients[ recordSetup ].addRegister( acWMS )
      for setup in self.dsClients:
        gLogger.info( "Sending records for setup %s" % setup )
        result = self.dsClients[ setup ].commit()
        if not result[ 'OK' ]:
          gLogger.error( "Couldn't commit wms history for setup %s"  % setup, result[ 'Message' ] )
        else:
          gLogger.info( "Sent %s records for setup %s" % ( result[ 'Value' ], setup ) )
    return S_OK()

