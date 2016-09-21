""" Test class for LSFTimeLeft utility

"""

import os
import unittest
from mock import MagicMock, patch

from DIRAC import gLogger, S_OK

from DIRAC.Core.Utilities.TimeLeft.LSFTimeLeft import LSFTimeLeft

LSF_KEK_BQUEUES = """   CPULIMIT
  720.0 min

  RUNLIMIT
  1440.0 min
"""

LSF_LSHOSTS = """ HOST_NAME                       type       model  cpuf ncpus maxmem maxswp server RESOURCES
b6688e710f                   SLC6_64 i6_16_63f2h24_266   2.5    16 29999M 19999M    Yes (intel share aishare cvmfs wan exe lcg wigner slot15)
  """

class LSFTimeLeftTest( unittest.TestCase ):
  """ test LSFTimeLeft """

  def setUp( self ):
    gLogger.setLevel( 'DEBUG' )

  def test_init( self ):

    rcMock = MagicMock()
    retValues = ( LSF_KEK_BQUEUES, LSF_LSHOSTS )
    rcMock.side_effect = ( S_OK( retValue ) for retValue in retValues )

    with patch( "DIRAC.Core.Utilities.TimeLeft.LSFTimeLeft.runCommand", new=rcMock ), \
         patch.dict( os.environ, {'LSB_HOSTS': 'b6688e710f'} ):
      lsfTimeLeft = LSFTimeLeft()
      self.assertEqual( lsfTimeLeft.cpuLimit, 720 * 60 / 2.5 )
      self.assertEqual( lsfTimeLeft.wallClockLimit, 1440 * 60 / 2.5 )
