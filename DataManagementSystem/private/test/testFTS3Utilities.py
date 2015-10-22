""" Test the FTS3Utilities"""
from DIRAC.DataManagementSystem.Client.FTS3File import FTS3File
from DIRAC import S_OK, S_ERROR

__RCSID__ = "$Id $"

import unittest
import mock
import datetime

from DIRAC.DataManagementSystem.private.FTS3Utilities import FTS3JSONDecoder, \
                                                             FTS3Serializable, \
                                                             groupFilesByTarget, \
                                                             generatePossibleTransfersBySources, \
                                                             selectUniqueSourceforTransfers


import json




class FakeClass( FTS3Serializable ):
  """ Just a fake class"""
  _attrToSerialize = ['string', 'date', 'dic', 'sub']

  def __init__( self ):
    self.string = ''
    self.date = None
    self.dic = {}

class TestFTS3Serialization( unittest.TestCase ):
  """ Test the FTS3 JSON serialization mechanizme with FTS3JSONEncoder,
      FTS3JSONDecoder, FTS3Serializable"""


  def test_01_basic( self ):
    """ Basic json transfer"""

    obj = FakeClass()
    obj.string = 'tata'
    obj.date = datetime.datetime.utcnow().replace( microsecond = 0 )
    obj.dic = { 'a' : 1}
    obj.notSerialized = 'Do not'


    obj2 = json.loads( obj.toJSON(), cls = FTS3JSONDecoder )


    self.assert_( obj.string == obj2.string )
    self.assert_( obj.date == obj2.date )
    self.assert_( obj.dic == obj2.dic )


    self.assert_( not hasattr( obj2, 'notSerialized' ) )


  def test_02_subobjects( self ):
    """ Try setting as attribute an object """

    class NonSerializable( object ):
      """ Fake class not inheriting from FTS3Serializable"""
      pass

    obj = FakeClass()
    obj.sub = NonSerializable()

    with self.assertRaises( TypeError ):
      obj.toJSON()

    obj.sub = FakeClass()
    obj.sub.string = 'pipo'

    obj2 = json.loads( obj.toJSON(), cls = FTS3JSONDecoder )

    self.assert_( obj.sub.string == obj2.sub.string )



def mock__checkSourceReplicas( ftsFiles ):
  succ = {}
  failed = {}

  for ftsFile in ftsFiles:
    if hasattr( ftsFile, 'fakeAttr_possibleSources' ):
      succ[ ftsFile.lfn] = dict.fromkeys( getattr( ftsFile, 'fakeAttr_possibleSources' ) )
    else:
      failed[ftsFile.lfn] = 'No such file or directory'


  return S_OK( {'Successful':succ, 'Failed':failed} )
  

class TestFileGrouping( unittest.TestCase ):
  """ Testing all the grouping functions of FTS3Utilities
  """


  def setUp( self ):
    self.f1 = FTS3File()
    self.f1.fakeAttr_possibleSources = ['Src1', 'Src2']
    self.f1.lfn = 'f1'
    self.f1.targetSE = 'target1'

    self.f2 = FTS3File()
    self.f2.fakeAttr_possibleSources = ['Src2', 'Src3']
    self.f2.lfn = 'f2'
    self.f2.targetSE = 'target2'

    self.f3 = FTS3File()
    self.f3.fakeAttr_possibleSources = ['Src4']
    self.f3.lfn = 'f3'
    self.f3.targetSE = 'target1'

    # File does not exist :-)
    self.f4 = FTS3File()
    self.f4.lfn = 'f4'
    self.f4.targetSE = 'target3'

    self.allFiles = [self.f1, self.f2, self.f3, self.f4 ]


  def test_01_groupFilesByTarget( self ):

    # empty input
    self.assert_( groupFilesByTarget( [] )['Value'] == {} )



    res = groupFilesByTarget( self.allFiles )
    
    self.assert_( res['OK'] )

    groups = res['Value']

    self.assert_( self.f1 in groups['target1'] )
    self.assert_( self.f2 in groups['target2'] )
    self.assert_( self.f3 in groups['target1'] )
    self.assert_( self.f4 in groups['target3'] )


  @mock.patch( 'DIRAC.DataManagementSystem.private.FTS3Utilities._checkSourceReplicas', side_effect = mock__checkSourceReplicas )
  def test_02_generatePossibleTransfersBySources( self, _mk_checkSourceReplicas ):
    """ Get all the possible sources"""
    # We assume here that they all go to the same target
    res = generatePossibleTransfersBySources( self.allFiles )

    self.assert_( res['OK'] )
    groups = res['Value']
    self.assert_( self.f1 in groups['Src1'] )
    self.assert_( self.f1 in groups['Src2'] )
    self.assert_( self.f2 in groups['Src2'] )
    self.assert_( self.f2 in groups['Src3'] )
    self.assert_( self.f3 in groups['Src4'] )
    self.assert_( self.f2 in groups['Src3'] )

  @mock.patch( 'DIRAC.DataManagementSystem.private.FTS3Utilities._checkSourceReplicas', side_effect = mock__checkSourceReplicas )
  def test_03_selectUniqueSourceforTransfers( self, _mk_checkSourceReplicas ):
    """ Suppose they all go to the same target """

    groupBySource = generatePossibleTransfersBySources( self.allFiles )['Value']

    res = selectUniqueSourceforTransfers( groupBySource )

    self.assert_( res['OK'] )

    uniqueSources = res['Value']
    # Src1 and Src2 should not be here because f1 and f2 should be taken from Src2
    self.assert_( sorted( uniqueSources.keys() ) == sorted( ['Src2', 'Src4'] ) )
    self.assert_( self.f1 in uniqueSources['Src2'] )
    self.assert_( self.f2 in uniqueSources['Src2'] )
    self.assert_( self.f3 in uniqueSources['Src4'] )




if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( TestFTS3Serialization )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestFileGrouping ) )
  unittest.TextTestRunner( verbosity = 2 ).run( suite )
