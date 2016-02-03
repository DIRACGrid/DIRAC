""" This is a test of the chain
    FTS3Client -> FTS3ManagerHandler -> FTS3DB

    It supposes that the DB is present, and that the service is running
"""

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

import unittest

from DIRAC import gLogger

from DIRAC.DataManagementSystem.Client.FTS3Client import FTS3Client
from DIRAC.DataManagementSystem.Client.FTS3Operation import FTS3Operation, FTS3TransferOperation,\
                                                            FTS3StagingOperation, FTS3RemovalOperation
from DIRAC.DataManagementSystem.Client.FTS3File import FTS3File
from DIRAC.DataManagementSystem.Client.FTS3Job import FTS3Job

from DIRAC.DataManagementSystem.DB.FTS3DB import FTS3DB

import time



class TestClientFTS3(unittest.TestCase):

  def setUp( self ):
    self.client = FTS3Client()
    self.fileCounter = 0
    
    
  def generateOperation( self, opType, nbFiles, dests, sources = None ):
    """ Generate one FTS3Operation object with FTS3Files in it"""
    op = None
    if opType == 'Transfer':
      op = FTS3TransferOperation()
    elif opType == 'Staging':
      op = FTS3StagingOperation()
    elif opType == 'Removal':
      op = FTS3RemovalOperation()
    op.username = "Pink"
    op.userGroup = "Floyd"
    op.sourceSEs = sources
    for _i in xrange(nbFiles*len(dests)):
      self.fileCounter += 1
      for dest in dests:
        ftsFile = FTS3File()
        ftsFile.lfn = 'lfn%s'%self.fileCounter
        ftsFile.targetSE = dest
        op.ftsFiles.append( ftsFile )
    
    return op
      
    
    
    
    
    
  def test_01_operation( self ):
    
    op = self.generateOperation( 'Transfer', 3, ['Target1', 'Target2' ], sources = ['Source1', 'Source2'] )

    self.assert_( not op.isTotallyProcessed() )

    res = self.client.persistOperation( op )
    self.assert_( res['OK'], res )
    opID = res['Value']

    res = self.client.getOperation( opID )
    self.assert_( res['OK'] )

    op2 = res['Value']
    


    self.assert_( isinstance( op2, FTS3TransferOperation ) )
    self.assert_( not op2.isTotallyProcessed() )
    
    for attr in ['username', 'userGroup', 'sourceSEs' ]:
      self.assert_( getattr( op, attr ) == getattr( op2, attr ) )

    self.assert_( len( op.ftsFiles ) == len( op2.ftsFiles ) )
    
    self.assert_( op2.status == FTS3Operation.INIT_STATE )

    fileIds = []
    for ftsFile in op2.ftsFiles:
      fileIds.append( ftsFile.fileID )
      self.assert_( ftsFile.status == FTS3File.INIT_STATE )

    # Testing the limit feature
    res = self.client.getOperationsWithFilesToSubmit( limit = 0 )

    self.assert_( res['OK'] )
    self.assert_( not res['Value'] )

    res = self.client.getOperationsWithFilesToSubmit()
    self.assert_( res['OK'] )
    self.assert_( len( res['Value'] ) == 1 )
    


    # Testing updating the status and error
    fileStatusDict = {}
    for fId in fileIds:
      fileStatusDict[fId] = { 'status' : 'Finished' if fId % 2 else 'Failed',
                             'error': '' if fId % 2 else 'Tough luck'}

    res = self.client.updateFileStatus( fileStatusDict )
    self.assert_( res['OK'] )

    res = self.client.getOperation( opID )
    op3 = res['Value']
    self.assert_( res['OK'] )
    
    self.assert_(op3.ftsFiles)
    for ftsFile in op3.ftsFiles:
      if ftsFile.fileID % 2:
        self.assert_( ftsFile.status == 'Finished' )
        self.assert_( not ftsFile.error )
      else:
        self.assert_( ftsFile.status == 'Failed' )
        self.assert_( ftsFile.error == 'Tough luck' )


    self.assert_( not op3.isTotallyProcessed() )


    # The operation still should be considered as having files to submit
    res = self.client.getOperationsWithFilesToSubmit()
    self.assert_( res['OK'] )
    self.assert_( len( res['Value'] ) == 1 )
    
    
    
    # Testing updating only the status and to final states
    fileStatusDict = {}
    nbFinalStates = len(FTS3File.FINAL_STATES)
    for fId in fileIds:
      fileStatusDict[fId] = { 'status' : FTS3File.FINAL_STATES[fId % nbFinalStates]}

    res = self.client.updateFileStatus( fileStatusDict )
    self.assert_( res['OK'] )

    res = self.client.getOperation( opID )
    op4 = res['Value']
    self.assert_( res['OK'] )
    
    self.assert_( op4.ftsFiles )
    for ftsFile in op4.ftsFiles:
      if ftsFile.fileID % 2:
        # Files to finished cannot be changed
        self.assert_( ftsFile.status == 'Finished' )
        self.assert_( not ftsFile.error )
      else:
        self.assert_( ftsFile.status == FTS3File.FINAL_STATES[ftsFile.fileID % nbFinalStates] )
        self.assert_( ftsFile.error == 'Tough luck' )


    # Now it should be considered as totally processed
    self.assert_( op4.isTotallyProcessed() )
    res = self.client.persistOperation( op4 )


    # The operation still should not be considered anymore
    res = self.client.getOperationsWithFilesToSubmit()
    self.assert_( res['OK'] )
    self.assert_( not res['Value'] )


    # The operation should be in Processed state in the DB
    # testing limit 0
    res = self.client.getProcessedOperations( limit = 0 )
    self.assert_( res['OK'] )
    self.assert_( not res['Value'] )

    res = self.client.getProcessedOperations()
    self.assert_( res['OK'] )
    self.assert_( len( res['Value'] ) == 1 )








  def test_02_job( self ):
    op = self.generateOperation( 'Transfer', 3, ['Target1', 'Target2' ], sources = ['Source1', 'Source2'] )

    job1 = FTS3Job()
    job1.ftsGUID = 'a-random-guid'
    job1.ftsServer = 'fts3'

    job1.username = "Pink"
    job1.userGroup = "Floyd"

    op.ftsJobs.append( job1 )

    res = self.client.persistOperation( op )
    self.assert_( res['OK'], res )
    opID = res['Value']

    res = self.client.getOperation( opID )
    self.assert_( res['OK'] )

    op2 = res['Value']
    self.assert_( len( op2.ftsJobs ) == 1 )
    job2 = op2.ftsJobs[0]
    self.assert_(job2.operationID == opID)
    
    for attr in ['ftsGUID', 'ftsServer', 'username', 'userGroup' ]:
      self.assert_( getattr( job1, attr ) == getattr( job2, attr ) )




  
  def _perf( self ):
    
    db = FTS3DB()
    listOfIds = []

    persistStart = time.time()
    for i in xrange( 1000 ):
      op = self.generateOperation( 'Transfer', i % 20 + 1, ['Dest1'] )
      res = db.persistOperation( op )
      self.assert_( res['OK'] )
      listOfIds.append( res['Value'] )

    persistEnd = time.time()
    
    for opId in listOfIds:
      res = db.getOperation( opId )
      self.assert_( res['OK'] )

    getEnd = time.time()

    print "Generation of 1000 operation and 10500 files %s s, retrival %s s" % ( persistEnd - persistStart, getEnd - persistEnd )






if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( TestClientFTS3 )

  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )

