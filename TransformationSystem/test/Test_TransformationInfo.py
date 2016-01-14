"""Test the Transformationinfo"""

import unittest
import sys
from StringIO import StringIO

from mock import MagicMock as Mock

from DIRAC import S_OK, S_ERROR, gLogger

from DIRAC.TransformationSystem.Utilities import TransformationInfo

__RCSID__ = "$Id$"


class TestTI(unittest.TestCase):
  """Test the TransformationInfo class"""

  def setUp(self):

    tMock = Mock(name="transMock")
    fcMock = Mock(name="fcMock")
    jmMock = Mock(name="jobMonMock")

    self.tri = TransformationInfo(transformationID=1234,
                                  transName="TestTrans",
                                  transType="MCGeneration",
                                  enabled=False,
                                  tClient=tMock,
                                  fcClient=fcMock,
                                  jobMon=jmMock,
                                  )

    self.taskDicts = [dict(TaskID=123,
                           LFN="lfn123",
                           Status="Assigned",
                           FileID=987001,
                           ),
                      dict(TaskID=124,
                           LFN="lfn124",
                           Status="Processed",
                           FileID=987002,
                           ),
                      ]

  def tearDown(self):
    pass

  def test_init(self):
    """DIRAC.TransformationSystem.Utilities.TransformationInfo init..........................."""
    self.assertIsInstance(self.tri, TransformationInfo)
    self.assertFalse(self.tri.enabled)

  def test_checkTasksStatus(self):
    """DIRAC.TransformationSystem.Utilities.TransformationInfo checkTasksStatus..............."""
    ## error getting files
    self.tri.tClient.getTransformationFiles.return_value = S_ERROR("nope")
    with self.assertRaisesRegexp(RuntimeError, "Failed to get transformation tasks: nope"):
      self.tri.checkTasksStatus()

    ## success getting files
    self.tri.tClient.getTransformationFiles.return_value = S_OK(self.taskDicts)
    retDict = self.tri.checkTasksStatus()
    self.assertEqual(len(retDict), 2)
    self.assertIn(123, retDict)
    self.assertIn(124, retDict)
    self.assertIn("FileID", retDict[124])

  def test_function(self):
    """DIRAC.TransformationSystem.Utilities.TransformationInfo function......................."""
    pass


if __name__ == "__main__":
  SUITE = unittest.defaultTestLoader.loadTestsFromTestCase(TestTI)
  TESTRESULT = unittest.TextTestRunner(verbosity=3).run(SUITE)

  # def test_function( self ):
  #   """DIRAC.TransformationSystem.Utilities.TransformationInfo function......................."""
  #   pass
