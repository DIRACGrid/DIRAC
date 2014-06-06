########################################################################
# $HeadURL$
# File :    StatesAccountingAgent.py
# Author :  A.T.
########################################################################

"""  StatesAccountingAgent sends periodically numbers of jobs in various states for various
     sites to the Monitoring system to create historical plots.
"""
__RCSID__ = "$Id$"


from DIRAC  import gLogger, gConfig, S_OK
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
from DIRAC.AccountingSystem.Client.Types.WMSHistory import WMSHistory
from DIRAC.AccountingSystem.Client.DataStoreClient import DataStoreClient
from DIRAC.Core.Utilities import Time

class StatesAccountingAgent( AgentModule ):
  """
      The specific agents must provide the following methods:
      - initialize() for initial settings
      - beginExecution()
      - execute() - the main method called in the agent cycle
      - endExecution()
      - finalize() - the graceful exit of the method, this one is usually used
                 for the agent restart
  """

  __summaryKeyFieldsMapping = [ 'Status',
                                'Site',
                                'User',
                                'UserGroup',
                                'JobGroup',
                                'JobType',
                              ]
  __summaryDefinedFields = [ ( 'ApplicationStatus', 'unset' ), ( 'MinorStatus', 'unset' ) ]
  __summaryValueFieldsMapping = [ 'Jobs',
                                  'Reschedules',
                                ]
  __renameFieldsMapping = { 'JobType' : 'JobSplitType' }


  def initialize( self ):
    """ Standard constructor
    """
    self.dsClients = {}
    self.jobDB = JobDB()

    self.reportPeriod = 850
    self.am_setOption( "PollingTime", self.reportPeriod )
    self.__jobDBFields = []
    for field in self.__summaryKeyFieldsMapping:
      if field == 'User':
        field = 'Owner'
      elif field == 'UserGroup':
        field = 'OwnerGroup'
      self.__jobDBFields.append( field )
    return S_OK()

  def execute( self ):
    """ Main execution method
    """
    result = gConfig.getSections( "/DIRAC/Setups" )
    if not result[ 'OK' ]:
      return result
    validSetups = result[ 'Value' ]
    gLogger.info( "Valid setups for this cycle are %s" % ", ".join( validSetups ) )
    #Get the WMS Snapshot!
    result = self.jobDB.getSummarySnapshot( self.__jobDBFields )
    now = Time.dateTime()
    if not result[ 'OK' ]:
      gLogger.error( "Can't the the jobdb summary", result[ 'Message' ] )
    else:
      values = result[ 'Value' ][1]
      for record in values:
        recordSetup = record[0]
        if recordSetup not in validSetups:
          gLogger.error( "Setup %s is not valid" % recordSetup )
          continue
        if recordSetup not in self.dsClients:
          gLogger.info( "Creating DataStore client for %s" % recordSetup )
          self.dsClients[ recordSetup ] = DataStoreClient( setup = recordSetup, retryGraceTime = 900 )
        record = record[1:]
        rD = {}
        for fV in self.__summaryDefinedFields:
          rD[ fV[0] ] = fV[1]
        for iP in range( len( self.__summaryKeyFieldsMapping ) ):
          fieldName = self.__summaryKeyFieldsMapping[iP]
          rD[ self.__renameFieldsMapping.get( fieldName, fieldName ) ] = record[iP]
        record = record[ len( self.__summaryKeyFieldsMapping ): ]
        for iP in range( len( self.__summaryValueFieldsMapping ) ):
          rD[ self.__summaryValueFieldsMapping[iP] ] = int( record[iP] )
        acWMS = WMSHistory()
        acWMS.setStartTime( now )
        acWMS.setEndTime( now )
        acWMS.setValuesFromDict( rD )
        retVal = acWMS.checkValues()
        if not retVal[ 'OK' ]:
          gLogger.error( "Invalid accounting record ", "%s -> %s" % ( retVal[ 'Message' ], rD ) )
        else:
          self.dsClients[ recordSetup ].addRegister( acWMS )
      for setup in self.dsClients:
        gLogger.info( "Sending records for setup %s" % setup )
        result = self.dsClients[ setup ].commit()
        if not result[ 'OK' ]:
          gLogger.error( "Couldn't commit wms history for setup %s" % setup, result[ 'Message' ] )
        else:
          gLogger.info( "Sent %s records for setup %s" % ( result[ 'Value' ], setup ) )
    return S_OK()

