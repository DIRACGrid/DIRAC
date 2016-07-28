#!/bin/env python
"""
tests for HTCondorCEComputingElement module
"""

import unittest
from mock import MagicMock as Mock, patch

from DIRAC.Resources.Computing import HTCondorCEComputingElement as HTCE
from DIRAC.Resources.Computing.BatchSystems import Condor
MODNAME = "DIRAC.Resources.Computing.HTCondorCEComputingElement"

STATUS_LINES = """
123.2 5
123.1 3
""".strip().split('\n')

HISTORY_LINES ="""
123 0 4
""".strip().split('\n')

class HTCondorCETests( unittest.TestCase ):
  """ tests for the HTCondorCE Module """

  def setUp( self ):
    pass

  def tearDown( self ):
    pass


  def test_parseCondorStatus( self ):
    statusLines = """
    104097.9 2
    104098.0 1
    104098.1 4
    104098.2 3
    104098.3 5
    104098.4 7
    """.strip().split('\n')

    expectedResults = {
      "104097.9": "Running",
      "104098.0": "Waiting",
      "104098.1": "Done",
      "104098.2": "Aborted",
      "104098.3": "HELD",
      "104098.4": "Unknown",
    }
    for jobID, expected in expectedResults.iteritems():
      self.assertEqual( HTCE.parseCondorStatus( statusLines, jobID ), expected )

  def test_getJobStatus( self ):

    htce = HTCE.HTCondorCEComputingElement( 12345 )

    with patch( MODNAME+".commands.getstatusoutput", new=Mock(
      side_effect=( [ (0, "\n".join(STATUS_LINES) ), # condor_q
                      (0, "\n".join(HISTORY_LINES)), # condor_history
                      (0, 0), # condor_rm, ignored in any case
                    ] ))), \
      patch( MODNAME+".HTCondorCEComputingElement._HTCondorCEComputingElement__cleanup", new=Mock() ) \
      :
      ret = htce.getJobStatus( ["htcondorce://condorce.foo.arg/123.0:::abc321",
                                "htcondorce://condorce.foo.arg/123.1:::c3b2a1",
                                "htcondorce://condorce.foo.arg/123.2:::c3b2a2",
                                "htcondorce://condorce.foo.arg/333.3:::c3b2a3",
                               ]
                             )

    expectedResults = { "htcondorce://condorce.foo.arg/123.0":"Done",
                        "htcondorce://condorce.foo.arg/123.1":"Aborted",
                        "htcondorce://condorce.foo.arg/123.2":"Aborted",
                        "htcondorce://condorce.foo.arg/333.3":"Unknown",
                      }

    self.assertTrue( ret['OK'] , ret.get('Message', '') )
    self.assertEqual( expectedResults, ret['Value'] )
    




class BatchCondorTest( unittest.TestCase ):
  """ tests for the plain batchSystem Condor Module """

  def test_getJobStatus( self ):

    with patch( MODNAME+".commands.getstatusoutput", new=Mock(
      side_effect=( [ (0, "\n".join(STATUS_LINES) ), # condor_q
                      (0, "\n".join(HISTORY_LINES)), # condor_history
                    ] ))):
      ret = Condor.Condor().getJobStatus( JobIDList = [ "123.0",
                                                        "123.1",
                                                        "123.2",
                                                        "333.3",
                                                      ]
                                        )

    expectedResults = { "123.0":"Done",
                        "123.1":"Aborted",
                        "123.2":"Unknown", ##HELD is treated as Unknown
                        "333.3":"Unknown",
                      }

    self.assertEqual( ret['Status'] , 0 )
    self.assertEqual( expectedResults, ret['Jobs'] )

if __name__ == '__main__':
  SUITE = unittest.defaultTestLoader.loadTestsFromTestCase( HTCondorCETests )
  SUITE.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( BatchCondorTest ) )
  unittest.TextTestRunner( verbosity = 2 ).run( SUITE )
