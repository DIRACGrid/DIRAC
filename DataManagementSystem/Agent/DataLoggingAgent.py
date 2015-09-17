'''
Created on Aug 26, 2015

@author: coberger
'''
from DIRAC import S_OK, gLogger

from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.DataManagementSystem.DB.DataLoggingDB    import DataLoggingDB

# # agent's name
AGENT_NAME = 'DataManagement/DataLoggingAgent'

########################################################################
class DataLoggingAgent( AgentModule ):
  """
  call moveSequences method of the DLS database to help services when there is insertion of DLCompressedSequence objects
  """
  def initialize( self ):
    self.maxSequenceToMove = self.am_getOption( "MaxSequenceToMove", 200 )
    self.__dataLoggingDB = DataLoggingDB( '/tmp/agentInsertion.txt', '/tmp/agentBetween.txt' )
    return S_OK()

  def execute( self ):
    """ this method call the moveSequences method of DataLoggingDB"""
    res = self.__dataLoggingDB.moveSequences( self.maxSequenceToMove )
    if not res['OK']:
      gLogger.error( 'DataLoggingAgent, error %s' % res['Message'] )
    return S_OK()