""" Test class for LSFTimeLeft utility

"""

import os
import unittest
from mock import MagicMock, patch

from DIRAC import gLogger, S_OK, S_ERROR

from DIRAC.Core.Utilities.TimeLeft.LSFTimeLeft import LSFTimeLeft

LSF_KEK_BQUEUES = """   CPULIMIT
  720.0 min

  RUNLIMIT
  1440.0 min
"""

LSF_LSHOSTS = """ HOST_NAME                       type       model  cpuf ncpus maxmem maxswp server RESOURCES
b6688e710f                   SLC6_64 i6_16_63f2h24_266   2.5    16 29999M 19999M    Yes (intel share aishare cvmfs wan exe lcg wigner slot15)
  """

LSF_CERN_BQUEUES = """   CPULIMIT
  10080.0 min of KSI2K

  RUNLIMIT
  30240.0 min of KSI2K
"""

#returns with S_ERROR
LSF_CERN_LSHOSTS_1= """KSI2K: unknown host name.
"""

##shortened
LSF_CERN_LSINFO= """MODEL_NAME      CPU_FACTOR      ARCHITECTURE
i6_12_62d7h20_266      3.06
ai_intel_8            2.44
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


  def test_init_cern( self ):

    rcMock = MagicMock()
    retValues = ( S_OK(LSF_CERN_BQUEUES), S_ERROR(LSF_CERN_LSHOSTS_1), S_OK(LSF_CERN_LSINFO), S_OK(LSF_LSHOSTS) )
    rcMock.side_effect = retValues
    sourceMock = MagicMock( return_value=S_ERROR( "no lsf.sh" ) )

    with patch( "DIRAC.Core.Utilities.TimeLeft.LSFTimeLeft.runCommand", new=rcMock ), \
         patch.dict( os.environ, {'LSB_HOSTS': 'b6688e710f', 'LSF_ENVDIR': "/dev/null"} ), \
         patch( "os.path.isfile", new=MagicMock( return_value=True ) ), \
         patch( "DIRAC.Core.Utilities.TimeLeft.LSFTimeLeft.sourceEnv", new=sourceMock ):
      lsfTimeLeft = LSFTimeLeft()
      normrefExpected = 1.0
      hostnormExpected = 2.5
      self.assertEqual( lsfTimeLeft.cpuLimit, 10080 * 60 / hostnormExpected / normrefExpected  )
      self.assertEqual( lsfTimeLeft.wallClockLimit, 30240 * 60 / hostnormExpected / normrefExpected )
      self.assertEqual( lsfTimeLeft.cpuRef, "KSI2K" )
      self.assertEqual( lsfTimeLeft.normRef, normrefExpected )
      self.assertEqual( lsfTimeLeft.hostNorm, hostnormExpected )
