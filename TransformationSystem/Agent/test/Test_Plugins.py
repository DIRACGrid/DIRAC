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

    self.util = importlib.import_module( 'DIRAC.TransformationSystem.Client.Utilities' )
    self.util.FileCatalog = self.mockCatalog
    self.util.StorageElement = MagicMock()

    self.maxDiff = None

    gLogger.setLevel( 'DEBUG' )

  def tearDown( self ):
#     sys.modules.pop( 'DIRAC.Core.Base.AgentModule' )
#     sys.modules.pop( 'DIRAC.TransformationSystem.Agent.TransformationAgent' )
    pass
  
  
class PluginsBaseSuccess( PluginsTestCase ):

  def test__Standard_G10( self ):
  #   # no input data, active
    params = dict( paramsBase )
    params['GroupSize'] = 10L
    pluginStandard = TransformationPlugin( 'Standard' )
    pluginStandard.setParameters( params )
    res = pluginStandard.run()
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], [] )

  def test__Standard_Data_G10( self ):
    # input data, active
    params = dict( paramsBase )
    params['GroupSize'] = 10L
    pluginStandard = TransformationPlugin( 'Standard' )
    pluginStandard.setParameters( params )
    pluginStandard.setInputData( data )
    res = pluginStandard.run()
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], [] )

  def test__Standard_Flush_G10( self ):
    # input data, flush
    pluginStandard = TransformationPlugin( 'Standard' )
    params = dict( paramsBase )
    params['GroupSize'] = 10L
    params['Status'] = 'Flush'
    pluginStandard.setParameters( params )
    pluginStandard.setInputData( data )
    res = pluginStandard.run()
    sortedData = [('SE1',
                   ['/this/is/also/at.12',
                    '/this/is/at.1',
                    '/this/is/at_123',
                    '/this/is/at.12']),
                  ('SE2', ['/this/is/als/at.2', '/this/is/at_23', '/this/is/at.2']),
                  ('SE4', ['/this/is/at_4'])]
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], sortedData )

  def test__Standard_G1( self ):
    #no input data, active
    pluginStandard = TransformationPlugin( 'Standard' )
    pluginStandard.setParameters( paramsBase )
    res = pluginStandard.run()
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], [] )

  def test__Standard_Data_G1( self ):
    # input data, active
    pluginStandard = TransformationPlugin( 'Standard' )
    pluginStandard.setParameters( paramsBase )
    pluginStandard.setInputData( data )
    res = pluginStandard.run()
    self.assert_( res['OK'] )
    sortedData = sorted([ (",".join(SEs), [lfn]) for lfn,SEs in data.iteritems() ])
    self.assertEqual( res['Value'], sortedData )

  def test__Standard_Flush_G1( self ):
    # input data, flush
    pluginStandard = TransformationPlugin( 'Standard' )
    params = dict( paramsBase )
    params['Status'] = 'Flush'
    pluginStandard.setParameters( params )
    pluginStandard.setInputData( data )
    res = pluginStandard.run()
    sortedData = sorted([ (",".join(SEs), [lfn]) for lfn,SEs in data.iteritems() ])
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], sortedData )

#############################################################################
# Test Suite run
#############################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( PluginsTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( PluginsBaseSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
