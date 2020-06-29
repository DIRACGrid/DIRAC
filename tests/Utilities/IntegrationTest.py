from __future__ import print_function
from __future__ import absolute_import
from __future__ import division
import unittest

from DIRAC.tests.Utilities.utils import cleanTestDir

from DIRAC import gLogger
from DIRAC.Interfaces.API.Dirac import Dirac
from DIRAC.DataManagementSystem.Client.DataManager import DataManager

class IntegrationTest( unittest.TestCase ):
  """ Base class for the integration and regression tests
  """

  def setUp( self ):
    cleanTestDir()
    self.dirac = Dirac()
    gLogger.setLevel( 'DEBUG' )

  def tearDown( self ):
#    cleanTestDir()
    pass


class FailingUserJobTestCase( IntegrationTest ):
  """ Base class for the faing jobs test cases
  """
  def setUp( self ):
    super( FailingUserJobTestCase, self ).setUp()

    dm = DataManager()
    res = dm.removeFile( ['/lhcb/testCfg/testVer/LOG/00012345/0006/00012345_00067890.tar',
                          '/lhcb/testCfg/testVer/SIM/00012345/0006/00012345_00067890_1.sim'],
                        force = True )
    if not res['OK']:
      print("Could not remove files", res['Message'])
      exit( 1 )
