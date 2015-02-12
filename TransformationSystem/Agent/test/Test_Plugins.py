""" Test class for plugins
"""

# imports
import unittest
import importlib
from mock import MagicMock

from DIRAC import gLogger

#sut
from DIRAC.TransformationSystem.Agent.TransformationPlugin import TransformationPlugin



paramsBase = {'AgentType': 'Automatic',
              'DerivedProduction': '0',
              'FileMask': '',
              'GroupSize': 1L,
              'InheritedFrom': 0L,
              'JobType': 'MCSimulation',
              'MaxNumberOfTasks': 0L,
              'OutputDirectories': "['/lhcb/MC/20', '/lhcb/debug/20']",
              'OutputLFNs': "{'LogTargetPath': ['/lhcb/9.tar'], 'LogFilePath': ['/lhcb/9']}",
              'Priority': '0',
              'SizeGroup': '1',
              'Status': 'Active',
              'TransformationID': 1080L,
              'Type': 'MCSimulation',
              'outputDataFileMask': 'GAUSSHIST;ALLSTREAMS.DST'}

data = {'/this/is/at.1':['SE1'],
        '/this/is/at.2':['SE2'],
        '/this/is/als/at.2':['SE2'],
        '/this/is/at.12':['SE1', 'SE2'],
        '/this/is/also/at.12':['SE1', 'SE2'],
        '/this/is/at_123':['SE1', 'SE2', 'SE3'],
        '/this/is/at_23':['SE2', 'SE3'],
        '/this/is/at_4':['SE4']}


class PluginsTestCase( unittest.TestCase ):
  """ Base class for the Agents test cases
  """
  def setUp( self ):
    self.mockTC = MagicMock()
    self.mockDM = MagicMock()
    self.mockCatalog = MagicMock()
    self.mockCatalog.getFileSize.return_value()
    self.tPlugin = importlib.import_module( 'DIRAC.TransformationSystem.Agent.TransformationPlugin' )
    self.tPlugin.TransformationClient = self.mockTC
    self.tPlugin.DataManager = self.mockDM
    self.tPlugin.FileCatalog = self.mockCatalog

    self.maxDiff = None

    gLogger.setLevel( 'DEBUG' )

  def tearDown( self ):
#     sys.modules.pop( 'DIRAC.Core.Base.AgentModule' )
#     sys.modules.pop( 'DIRAC.TransformationSystem.Agent.TransformationAgent' )
    pass
  
  
class PluginsBaseSuccess( PluginsTestCase ):

  def test__Standard( self ):
    # no input data, active
    pluginStandard = TransformationPlugin( 'Standard' )
    pluginStandard.setParameters( paramsBase )
    res = pluginStandard.generateTasks()
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], [] )

    # input data, active
    pluginStandard = TransformationPlugin( 'Standard' )
    pluginStandard.setParameters( paramsBase )
    pluginStandard.setInputData( data )
    res = pluginStandard.generateTasks()
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], [] )

    # input data, flush
    pluginStandard = TransformationPlugin( 'Standard' )
    paramsBase['Status'] = 'Flush'
    pluginStandard.setParameters( paramsBase )
    pluginStandard.setInputData( data )
    res = pluginStandard.generateTasks()
    self.assert_( res['OK'] )
    self.assertNotEqual( res['Value'], [] )

#############################################################################
# Test Suite run
#############################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( PluginsTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( PluginsBaseSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
