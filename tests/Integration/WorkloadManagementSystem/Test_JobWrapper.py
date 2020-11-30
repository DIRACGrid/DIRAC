""" JobWrapper test
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest
import os
import sys

from DIRAC import gLogger

from DIRAC.Resources.Computing.ComputingElementFactory import ComputingElementFactory
from DIRAC.WorkloadManagementSystem.Utilities.Utils import createJobWrapper
from DIRAC.Core.Security.ProxyInfo import getProxyInfo


class JobWrapperTestCase(unittest.TestCase):
  """ Base class for the jobWrapper test cases
  """

  def setUp(self):
    gLogger.setLevel('DEBUG')
    self.wrapperFile = None

    # get proxy
    proxyInfo = getProxyInfo(disableVOMS=True)
    proxyChain = proxyInfo['Value']['chain']
    proxyDumped = proxyChain.dumpAllToString()
    self.payloadProxy = proxyDumped['Value']

  def tearDown(self):
    pass


class JobWrapperSubmissionCase(JobWrapperTestCase):
  """  JobWrapperSubmissionCase represents a test suite for
  """

  def test_CreateAndSubmit(self):

    jobParams = {'JobID': '1',
                 'JobType': 'Merge',
                 'CPUTime': '1000000',
                 'Executable': '$DIRACROOT/scripts/dirac-jobexec',
		 'Arguments': "helloWorld.xml -o LogLevel=DEBUG --cfg pilot.cfg",
                 'InputSandbox': ['helloWorld.xml', 'exe-script.py']}
    resourceParams = {}
    optimizerParams = {}

#     res = createJobWrapper( 1, jobParams, resourceParams, optimizerParams, logLevel = 'DEBUG' )
#     self.assertTrue( res['OK'] )
#     wrapperFile = res['Value']

    ceFactory = ComputingElementFactory()
    ceInstance = ceFactory.getCE('InProcess')
    self.assertTrue(ceInstance['OK'])
    computingElement = ceInstance['Value']

#     res = computingElement.submitJob( wrapperFile, self.payloadProxy )
#     self.assertTrue( res['OK'] )

    if 'pilot.cfg' in os.listdir('.'):
      jobParams.setdefault('ExtraOptions', 'pilot.cfg')
      res = createJobWrapper(2, jobParams, resourceParams, optimizerParams, extraOptions='pilot.cfg', logLevel='DEBUG')
    else:
      res = createJobWrapper(2, jobParams, resourceParams, optimizerParams, logLevel='DEBUG')
    self.assertTrue(res['OK'], res.get('Message'))
    wrapperFile = res['Value']

    res = computingElement.submitJob(wrapperFile, self.payloadProxy)
    self.assertTrue(res['OK'], res.get('Message'))


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(JobWrapperTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(JobWrapperSubmissionCase))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
  sys.exit(not testResult.wasSuccessful())
