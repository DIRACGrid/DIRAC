########################################################################
# $HeadURL$
# File :    StatesMonitoringAgent.py
# Author :  Zoltan Mathe
########################################################################

"""  StatesMonitoringAgent sends periodically numbers of jobs in various states for various
     sites to the Monitoring system to create historical plots.
"""
__RCSID__ = "$Id$"

import json

from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
from DIRAC.Core.Utilities import Time
from DIRAC.Resources.MessageQueue.MQPublisher import MQPublisher



class StatesMonitoringAgent( AgentModule ):
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
  __summaryValueFieldsMapping = [ 'value',
                                  'Reschedules',
                                ]
  __renameFieldsMapping = { 'JobType' : 'JobSplitType' }


  def initialize( self ):
    """ Standard constructor
    """
    
    self.jobDB = JobDB()

    self.am_setOption( "PollingTime", 120 )
    self.__jobDBFields = []
    for field in self.__summaryKeyFieldsMapping:
      if field == 'User':
        field = 'Owner'
      elif field == 'UserGroup':
        field = 'OwnerGroup'
      self.__jobDBFields.append( field )
    
    self.publisher = MQPublisher( "TestQueue" )
    return S_OK()

  def sendRecords( self, data, retry = None ):
    
    if retry > 3:
      return S_ERROR("can not send the data to MQ!")
    
    try:
      self.publisher.put(data)
    except Exception as e:
      retry += 1
      gLogger.error( "Connection closed:" + repr( e ) )
      self.sendRecords( data )
      
  def finalize( self ):
    self.publisher.stop()
    
  def execute( self ):
    """ Main execution method
    """
    result = gConfig.getSections( "/DIRAC/Setups" )
    if not result[ 'OK' ]:
      return result
    validSetups = result[ 'Value' ]
    gLogger.info( "Valid setups for this cycle are %s" % ", ".join( validSetups ) )
    # Get the WMS Snapshot!
    result = self.jobDB.getSummarySnapshot( self.__jobDBFields )
    now = Time.dateTime()
    if not result[ 'OK' ]:
      gLogger.error( "Can't the the jobdb summary", result[ 'Message' ] )
    else:
      values = result[ 'Value' ][1]
      gLogger.info( "Start sending records!" )
      for record in values:
        recordSetup = record[0]
        if recordSetup not in validSetups:
          gLogger.error( "Setup %s is not valid" % recordSetup )
          continue
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
        rD['startTime'] = int( Time.toEpoch( now ) )       
        rD['metric'] = 'WMSHistory'
        message = json.dumps( rD )
        self.sendRecords( message )
      gLogger.info( "The records are successfully sent!" )
        
    return S_OK()

