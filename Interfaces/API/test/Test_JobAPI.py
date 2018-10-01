""" Basic unit tests for the Job API
"""

__RCSID__ = "$Id$"

import unittest
import StringIO

from DIRAC.Interfaces.API.Job import Job
from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd


class JobAPITestCase(unittest.TestCase):
  """ Base class for the Modules test cases
  """

  def setUp(self):
    self.job = Job()

  def tearDown(self):
    pass


class JobAPISuccess(JobAPITestCase):

  def test_basicJob(self):
    self.job.setOwner('ownerName')
    self.job.setOwnerGroup('ownerGroup')
    self.job.setName('jobName')
    self.job.setJobGroup('jobGroup')
    self.job.setExecutable('someExe')
    self.job.setType('jobType')
    self.job.setDestination('ANY')

    xml = self.job._toXML()

    try:
      with open('./DIRAC/Interfaces/API/test/testWF.xml') as fd:
        expected = fd.read()
    except IOError:
      with open('./Interfaces/API/test/testWF.xml') as fd:
        expected = fd.read()

    self.assertEqual(xml, expected)

    self.job._toJDL(jobDescriptionObject=StringIO.StringIO(self.job._toXML()))

  def test_SimpleParametricJob(self):

    job = Job()
    job.setExecutable('myExec')
    job.setLogLevel('DEBUG')
    parList = [1, 2, 3]
    job.setParameterSequence('JOB_ID', parList, addToWorkflow=True)
    inputDataList = [
        [
            '/lhcb/data/data1',
            '/lhcb/data/data2'
        ],
        [
            '/lhcb/data/data3',
            '/lhcb/data/data4'
        ],
        [
            '/lhcb/data/data5',
            '/lhcb/data/data6'
        ]
    ]
    job.setParameterSequence('InputData', inputDataList, addToWorkflow=True)

    jdl = job._toJDL()

    print jdl

    clad = ClassAd('[' + jdl + ']')

    arguments = clad.getAttributeString('Arguments')
    job_id = clad.getAttributeString('JOB_ID')
    inputData = clad.getAttributeString('InputData')

    print "arguments", arguments

    self.assertEqual(job_id, '%(JOB_ID)s')
    self.assertEqual(inputData, '%(InputData)s')
    self.assertIn('jobDescription.xml', arguments)
    self.assertIn('-o LogLevel=DEBUG', arguments)
    self.assertIn('-p JOB_ID=%(JOB_ID)s', arguments)
    self.assertIn('-p InputData=%(InputData)s', arguments)


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(JobAPISuccess)
  # suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( xxxx ) )

  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
