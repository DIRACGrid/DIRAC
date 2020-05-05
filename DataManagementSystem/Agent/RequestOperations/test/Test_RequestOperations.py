from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import unittest
import datetime
import json
from mock import MagicMock


from DIRAC.DataManagementSystem.Agent.RequestOperations.ReplicateAndRegister import ReplicateAndRegister
from DIRAC.DataManagementSystem.Agent.RequestOperations.MoveReplica import MoveReplica
from DIRAC.RequestManagementSystem.Client.File import File
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.Client.Request import Request

class ReqOpsTestCase( unittest.TestCase ):
  """ Base class for the clients test cases
  """
  def setUp( self ):
    fcMock = MagicMock()
    ftsMock = MagicMock()

    self.rr = ReplicateAndRegister()
    self.rr.fc = fcMock
    self.rr.ftsClient = ftsMock

  def tearDown( self ):
    pass

#############################################################################
class MoveReplicaSuccess( ReqOpsTestCase ):

  def setUp(self):
    self.op = Operation()
    self.op.Type = "MoveFile"
    self.op.SourceSE = "%s,%s" % ( "sourceSE1", "sourceSE2" )
    self.op.TargetSE = "%s,%s" % ( "targetSE1", "targetSE2" )

    self.File = File()
    self.File.LFN = '/cta/file1'
    self.File.Size = 2
    self.File.Checksum = '011300a2'
    self.File.ChecksumType = "adler32"
    self.op.addFile( self.File )

    self.req = Request()
    self.req.addOperation( self.op )
    self.mr = MoveReplica( self.op )

    self.mr.dm = MagicMock()
    self.mr.fc = MagicMock()

  # This test needs to be fixed. It currently fails because StorageElement is not mocked
  '''def test__dmTransfer( self ):

    successful = {}
    for sourceSE in self.op.sourceSEList:
      successful[sourceSE] = 'dips://' + sourceSE.lower() + ':9148/DataManagement/StorageElement' + self.File.LFN

    res = {'OK': True, 'Value': {'Successful': {self.File.LFN : successful}, 'Failed': {}}}
    self.mr.dm.getActiveReplicas.return_value = res

    res = {'OK': True, 'Value': {'Successful': {self.File.LFN : {'register': 0.1228799819946289, 'replicate': 9.872732877731323}}, 'Failed': {}}}
    self.mr.dm.replicateAndRegister.return_value = res

    res = self.mr.dmTransfer( self.File )
    self.assertTrue( res['OK'] )

    self.assertEqual( self.mr.operation.__files__[0].Status, 'Waiting' )
    self.assertEqual( self.mr.operation.Status, 'Waiting' )
    self.assertEqual( self.mr.request.Status, 'Waiting' )'''

  def test__dmRemoval( self ):

    res = {'OK': True, 'Value': {'Successful': { self.File.LFN : {'DIRACFileCatalog': True}}, 'Failed': {}}}
    self.mr.dm.removeReplica.return_value = res

    toRemoveDict = {self.File.LFN: self.File}
    targetSEs = self.op.sourceSEList

    res = self.mr.dmRemoval( toRemoveDict, targetSEs )
    self.assertTrue( res['OK'] )

    resvalue = dict( [ ( targetSE, '' ) for targetSE in targetSEs ] )
    self.assertEqual( res['Value'], {self.File.LFN: resvalue} )

    self.assertEqual( self.mr.operation.__files__[0].Status, 'Done' )
    self.assertEqual( self.mr.operation.Status, 'Done' )
    self.assertEqual( self.mr.request.Status, 'Done' )

class MoveReplicaFailure( ReqOpsTestCase ):

  def setUp( self ):
    self.op = Operation()
    self.op.Type = "MoveReplica"
    self.op.SourceSE = "%s,%s" % ( "sourceSE1", "sourceSE2" )
    self.op.TargetSE = "%s,%s" % ( "targetSE1", "targetSE2" )

    self.File = File()
    self.File.LFN = '/cta/file1'
    self.File.Size = 2
    self.File.Checksum = '011300a2'
    self.File.ChecksumType = "adler32"
    self.op.addFile( self.File )

    self.req = Request()
    self.req.addOperation( self.op )
    self.mr = MoveReplica( self.op )

    self.mr.dm = MagicMock()
    self.mr.fc = MagicMock()
    self.mr.ci = MagicMock()

  def test__dmTransfer( self ):

    successful = {}
    for sourceSE in self.op.sourceSEList:
      successful[sourceSE] = 'dips://' + sourceSE.lower() + ':9148/DataManagement/StorageElement' + self.File.LFN

    res = {'OK': True, 'Value': ({self.File.LFN: successful}, [])}
    self.mr.ci._getCatalogReplicas.return_value = res

    res = {'OK': True, 'Value': {'MissingAllReplicas': {}, 'NoReplicas': {}, 'MissingReplica': {}, 'SomeReplicasCorrupted': {}, 'AllReplicasCorrupted': {}}}
    self.mr.ci.compareChecksum.return_value = res

    res =  {'OK': True, 'Value': {'Successful': {}, 'Failed': {self.File.LFN : 'Unable to replicate file'}}}
    self.mr.dm.replicateAndRegister.return_value = res

    res = self.mr.dmTransfer( self.File )
    self.assertFalse( res['OK'] )

    self.assertEqual( self.mr.operation.__files__[0].Status, 'Waiting' )
    self.assertEqual( self.mr.operation.Status, 'Waiting' )
    self.assertEqual( self.mr.request.Status, 'Waiting' )


  def test__dmRemoval( self ):

    res =  {'OK': True, 'Value': {'Successful': {}, 'Failed': {self.File.LFN: 'Write access not permitted for this credential'}}}
    self.mr.dm.removeReplica.return_value = res

    toRemoveDict = {self.File.LFN: self.File}
    targetSEs = self.op.sourceSEList

    res = self.mr.dmRemoval( toRemoveDict, targetSEs )
    self.assertTrue( res['OK'] )

    resvalue = dict( [ ( targetSE, 'Write access not permitted for this credential' ) for targetSE in targetSEs ] )
    self.assertEqual( res['Value'], {self.File.LFN: resvalue} )

    self.assertEqual( self.mr.operation.__files__[0].Status, 'Waiting' )
    self.assertEqual( self.mr.operation.Status, 'Waiting' )
    self.assertEqual( self.mr.request.Status, 'Waiting' )

class ReplicateAndRegisterSuccess( ReqOpsTestCase ):

  def test__addMetadataToFiles( self ):
    resMeta = {'OK': True,
     'Value': {'Failed': {},
               'Successful': {'/lhcb/1.dst': {'ChecksumType': 'AD',
                                              'Checksum': '123456',
                                              'CreationDate': datetime.datetime( 2013, 12, 11, 20, 20, 21 ),
                                              'GUID': '92F9CE97-7A62-E311-8401-0025907FD430',
                                              'Mode': 436,
                                              'ModificationDate': datetime.datetime( 2013, 12, 11, 20, 20, 21 ),
                                              'NumberOfLinks': 1,
                                              'Size': 5846023777,
                                              'Status': '-'},
                              '/lhcb/2.dst': {'ChecksumType': 'AD',
                                              'Checksum': '987654',
                                              'CreationDate': datetime.datetime( 2013, 12, 12, 6, 26, 52 ),
                                              'GUID': 'DAE4933A-C162-E311-8A6B-003048FEAF04',
                                              'Mode': 436,
                                              'ModificationDate': datetime.datetime( 2013, 12, 12, 6, 26, 52 ),
                                              'NumberOfLinks': 1,
                                              'Size': 5893396937,
                                              'Status': '-'}}}}

    self.rr.fc.getFileMetadata.return_value = resMeta

    file1 = File()
    file1.LFN = '/lhcb/1.dst'
    file2 = File()
    file2.LFN = '/lhcb/2.dst'

    toSchedule = {'/lhcb/1.dst': [file1, ['SE1'], ['SE2', 'SE3']],
                  '/lhcb/2.dst': [file2, ['SE4'], ['SE5', 'SE6']]}

    res = self.rr._addMetadataToFiles( toSchedule )
    self.assertTrue(res['OK'])

    for lfn in toSchedule:
      self.assertEqual( res['Value'][lfn].LFN, lfn )
      for attr in ('GUID', 'Size', 'Checksum'):
        self.assertEqual( getattr(res['Value'][lfn],attr), resMeta['Value']['Successful'][lfn][attr] )
      # AD should be transformed into Adler32
      self.assertEqual( res['Value'][lfn].ChecksumType, "ADLER32" )


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( ReqOpsTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ReplicateAndRegisterSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( MoveReplicaSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( MoveReplicaFailure ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
