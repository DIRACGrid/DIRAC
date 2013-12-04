# import unittest, datetime, sys
#
# from mock import Mock
#
# from DIRAC.ResourceStatusSystem.Agent.mock.AgentModule import AgentModule as mockAgentModule
#
# class AgentsTestCase( unittest.TestCase ):
#  """ Base class for the Agents test cases
#  """
#  def setUp( self ):
#    self.mockTC = Mock()
#    import DIRAC.Core.Base.AgentModule as moduleMocked
#    moduleMocked.AgentModule = mockAgentModule
#    from DIRAC.TransformationSystem.Agent.TransformationAgent import TransformationAgent
# #    import DIRAC.TransformationSystem.Agent.TransformationAgent as mockedModule
# #    mockedModule.TransformationAgent.__bases__ = ( mockAgentModule, mockedModule.TransformationAgent.__bases__[1] )
#    self.ta = TransformationAgent( 'ta', 'ta' )
#    self.ta.transfClient = self.mockTC
#
#
#  def tearDown( self ):
#    sys.modules.pop( 'DIRAC.Core.Base.AgentModule' )
#    sys.modules.pop( 'DIRAC.TransformationSystem.Agent.TransformationAgent' )
#    pass
#
# class TransformationAgentSuccess( AgentsTestCase ):
#
#  def test__getTransformationFiles( self ):
#    goodFiles = {'OK':True,
#                 'Value':[{'ErrorCount': 1L,
#                           'FileID': 17990660L,
#                           'InsertedTime': datetime.datetime( 2012, 3, 15, 17, 5, 50 ),
#                           'LFN': '/00012574_00000239_1.charmcompleteevent.dst',
#                           'LastUpdate': datetime.datetime( 2012, 3, 16, 23, 43, 26 ),
#                           'RunNumber': 90269L,
#                           'Status': 'Unused',
#                           'TargetSE': 'Unknown',
#                           'TaskID': '222',
#                           'TransformationID': 17042L,
#                           'UsedSE': 'CERN-DST,IN2P3_M-DST,PIC-DST,RAL-DST'},
#                          {'ErrorCount': 1L,
#                           'FileID': 17022945L,
#                           'InsertedTime': datetime.datetime( 2012, 3, 15, 17, 5, 50 ),
#                           'LFN': '/00012574_00000119_1.charmcompleteevent.dst',
#                           'LastUpdate': datetime.datetime( 2012, 3, 16, 23, 54, 59 ),
#                           'RunNumber': 90322L,
#                           'Status': 'Unused',
#                           'TargetSE': 'Unknown',
#                           'TaskID': '82',
#                           'TransformationID': 17042L,
#                           'UsedSE': 'CERN-DST,CNAF-DST,RAL_M-DST,SARA-DST'}]
#                 }
#    noFiles = {'OK':True, 'Value':[]}
#
#    for getTFiles in [goodFiles, noFiles]:
#      self.mockTC.getTransformationFiles.return_value = getTFiles
#
#      transDict = {'TransformationID': 123, 'Status': 'Stopped' }
#      res = self.ta._getTransformationFiles( transDict, {'TransformationClient': self.mockTC} )
#
#      self.assertTrue( res['OK'] )
#
#
#
##############################################################################
# # Test Suite run
##############################################################################
#
# if __name__ == '__main__':
#  suite = unittest.defaultTestLoader.loadTestsFromTestCase( AgentsTestCase )
#  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TransformationAgentSuccess ) )
#  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
#
# #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
