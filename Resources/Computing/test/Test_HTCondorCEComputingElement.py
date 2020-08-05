#!/bin/env python
"""
tests for HTCondorCEComputingElement module
"""

import unittest
from mock import MagicMock as Mock, patch
from parameterized import parameterized, param

from DIRAC.Resources.Computing import HTCondorCEComputingElement as HTCE
from DIRAC.Resources.Computing.BatchSystems import Condor
from DIRAC import S_OK

MODNAME = "DIRAC.Resources.Computing.HTCondorCEComputingElement"

STATUS_LINES = """
123.2 5
123.1 3
""".strip().split('\n')

HISTORY_LINES = """
123 0 4
""".strip().split('\n')


class HTCondorCETests(unittest.TestCase):
  """ tests for the HTCondorCE Module """

  def setUp(self):
    self.ceParameters = {'Queue': "espresso",
                         'GridEnv': "/dev/null"}

  def tearDown(self):
    pass

  def test_parseCondorStatus(self):
    statusLines = """
    104097.9 2
    104098.0 1
    104098.1 4

    foo bar
    104098.2 3
    104098.3 5
    104098.4 7
    """.strip().split('\n')
    # force there to be an empty line

    expectedResults = {"104097.9": "Running",
                       "104098.0": "Waiting",
                       "104098.1": "Done",
                       "104098.2": "Aborted",
                       "104098.3": "HELD",
                       "104098.4": "Unknown"}

    for jobID, expected in expectedResults.iteritems():
      self.assertEqual(HTCE.parseCondorStatus(statusLines, jobID), expected)

  @patch(MODNAME + ".commands.getstatusoutput", new=Mock(side_effect=([(0, "\n".join(STATUS_LINES)),
                                                                       (0, "\n".join(HISTORY_LINES)),
                                                                       (0, 0)])))
  @patch(MODNAME + ".HTCondorCEComputingElement._HTCondorCEComputingElement__cleanup", new=Mock())
  def test_getJobStatus(self):

    htce = HTCE.HTCondorCEComputingElement(12345)

    ret = htce.getJobStatus(["htcondorce://condorce.foo.arg/123.0:::abc321",
                             "htcondorce://condorce.foo.arg/123.1:::c3b2a1",
                             "htcondorce://condorce.foo.arg/123.2:::c3b2a2",
                             "htcondorce://condorce.foo.arg/333.3:::c3b2a3"])

    expectedResults = {"htcondorce://condorce.foo.arg/123.0": "Done",
                       "htcondorce://condorce.foo.arg/123.1": "Aborted",
                       "htcondorce://condorce.foo.arg/123.2": "Aborted",
                       "htcondorce://condorce.foo.arg/333.3": "Unknown"}

    self.assertTrue(ret['OK'], ret.get('Message', ''))
    self.assertEqual(expectedResults, ret['Value'])

  def test__writeSub_local(self):
    htce = HTCE.HTCondorCEComputingElement(12345)
    htce.useLocalSchedd = True
    subFileMock = Mock()

    patchFdopen = patch(MODNAME + ".os.fdopen", new=Mock(return_value=subFileMock))
    patchMkstemp = patch(MODNAME + ".tempfile.mkstemp", new=Mock(return_value=("os", "pilotName")))
    patchMkdir = patch(MODNAME + ".mkDir", new=Mock())

    with patchFdopen, patchMkstemp, patchMkdir:
      htce._HTCondorCEComputingElement__writeSub("dirac-install", 42, '', 1)  # pylint: disable=E1101
      for option in ["ShouldTransferFiles = YES", "WhenToTransferOutput = ON_EXIT_OR_EVICT", "universe = grid"]:
        # the three [0] are: call_args_list[firstCall][ArgsArgumentsTuple][FirstArgsArgument]
        self.assertIn(option, subFileMock.write.call_args_list[0][0][0])

  def test__writeSub_remote(self):
    htce = HTCE.HTCondorCEComputingElement(12345)
    htce.useLocalSchedd = False
    subFileMock = Mock()

    patchFdopen = patch(MODNAME + ".os.fdopen", new=Mock(return_value=subFileMock))
    patchMkstemp = patch(MODNAME + ".tempfile.mkstemp", new=Mock(return_value=("os", "pilotName")))
    patchMkdir = patch(MODNAME + ".mkDir", new=Mock())

    with patchFdopen, patchMkstemp, patchMkdir:
      htce._HTCondorCEComputingElement__writeSub("dirac-install", 42, '', 1)  # pylint: disable=E1101
      for option in ["ShouldTransferFiles = YES", "WhenToTransferOutput = ON_EXIT_OR_EVICT"]:
        self.assertNotIn(option, subFileMock.write.call_args_list[0][0][0])
      for option in ["universe = vanilla"]:
        self.assertIn(option, subFileMock.write.call_args_list[0][0][0])

  def test_reset_local(self):
    htce = HTCE.HTCondorCEComputingElement(12345)
    htce.ceParameters = self.ceParameters
    htce.useLocalSchedd = True
    ceName = "condorce.cern.ch"
    htce.ceName = ceName
    htce._reset()
    self.assertEqual(htce.remoteScheddOptions, "")

  def test_reset_remote(self):
    htce = HTCE.HTCondorCEComputingElement(12345)
    htce.ceParameters = self.ceParameters
    htce.useLocalSchedd = False
    ceName = "condorce.cern.ch"
    htce.ceName = ceName
    htce._reset()
    self.assertEqual(htce.remoteScheddOptions, "-pool %s:9619 -name %s " % (ceName, ceName))

  def test_submitJob_local(self):
    htce = HTCE.HTCondorCEComputingElement(12345)
    htce.ceParameters = self.ceParameters
    htce.useLocalSchedd = True
    ceName = "condorce.cern.ch"
    htce.ceName = ceName
    execMock = Mock(return_value=S_OK((0, "123.0 - 123.0")))
    htce._HTCondorCEComputingElement__writeSub = Mock(return_value="dirac-pilot")
    with patch(MODNAME + ".executeGridCommand", new=execMock), patch(MODNAME + ".os", new=Mock()):
      result = htce.submitJob("pilot", "proxy", 1)

    self.assertTrue(result['OK'], result.get('Message'))
    remotePoolList = " ".join(['-pool', '%s:9619' % ceName, '-remote', ceName])
    self.assertNotIn(remotePoolList, " ".join(execMock.call_args_list[0][0][1]))

  def test_submitJob_remote(self):
    htce = HTCE.HTCondorCEComputingElement(12345)
    htce.ceParameters = self.ceParameters
    htce.useLocalSchedd = False
    ceName = "condorce.cern.ch"
    htce.ceName = ceName
    execMock = Mock(return_value=S_OK((0, "123.0 - 123.0")))
    htce._HTCondorCEComputingElement__writeSub = Mock(return_value="dirac-pilot")
    with patch(MODNAME + ".executeGridCommand", new=execMock), patch(MODNAME + ".os", new=Mock()):
      result = htce.submitJob("pilot", "proxy", 1)

    self.assertTrue(result['OK'], result.get('Message'))
    remotePoolList = " ".join(['-pool', '%s:9619' % ceName, '-remote', ceName])
    self.assertIn(remotePoolList, " ".join(execMock.call_args_list[0][0][1]))

  @parameterized.expand([param([], ''),
                         param('', ''),
                         param(['htcondorce://condorce.foo.arg/123.0:::abc321'], '123.0'),
                         param('htcondorce://condorce.foo.arg/123.0:::abc321', '123.0'),
                         param('htcondorce://condorce.foo.arg/123.0:::abc321', '123.0', ret=1, success=False),
                         param(['htcondorce://condorce.foo.arg/333.3'], '333.3'),
                         param('htcondorce://condorce.foo.arg/333.3', '333.3', local=False),
                         ])
  def test_killJob(self, jobIDList, jobID, ret=0, success=True, local=True):
    mock = Mock(return_value=(ret, ''))
    htce = HTCE.HTCondorCEComputingElement(12345)
    htce.ceName = 'condorce.foo.arg'
    htce.useLocalSchedd = local
    htce.ceParameters = self.ceParameters
    htce._reset()
    with patch(MODNAME + ".commands.getstatusoutput", new=mock):
      ret = htce.killJob(jobIDList=jobIDList)

    assert ret['OK'] == success
    if jobID:
      mock.assert_called_with('condor_rm %s %s' % (htce.remoteScheddOptions, jobID))


class BatchCondorTest(unittest.TestCase):
  """ tests for the plain batchSystem Condor Module """

  def test_getJobStatus(self):
    mock = Mock(side_effect=([(0, "\n".join(STATUS_LINES)),  # condor_q
                              (0, "\n".join(HISTORY_LINES))]))  # condor_history

    with patch(MODNAME + ".commands.getstatusoutput", new=mock):
      ret = Condor.Condor().getJobStatus(JobIDList=["123.0",
                                                    "123.1",
                                                    "123.2",
                                                    "333.3"])

    expectedResults = {"123.0": "Done",
                       "123.1": "Aborted",
                       "123.2": "Unknown",  # HELD is treated as Unknown
                       "333.3": "Unknown"}

    self.assertEqual(ret['Status'], 0)
    self.assertEqual(expectedResults, ret['Jobs'])


if __name__ == '__main__':
  SUITE = unittest.defaultTestLoader.loadTestsFromTestCase(HTCondorCETests)
  SUITE.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(BatchCondorTest))
  unittest.TextTestRunner(verbosity=2).run(SUITE)
