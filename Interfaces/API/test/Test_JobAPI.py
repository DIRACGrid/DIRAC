""" Basic unit tests for the Job API
"""

__RCSID__ = "$Id$"

import StringIO

from DIRAC.Interfaces.API.Job import Job
from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd


def test_basicJob():
  job = Job()

  job.setOwner('ownerName')
  job.setOwnerGroup('ownerGroup')
  job.setName('jobName')
  job.setJobGroup('jobGroup')
  job.setExecutable('someExe')
  job.setType('jobType')
  job.setDestination('ANY')

  xml = job._toXML()

  try:
    with open('./DIRAC/Interfaces/API/test/testWF.xml') as fd:
      expected = fd.read()
  except IOError:
    with open('./Interfaces/API/test/testWF.xml') as fd:
      expected = fd.read()

  assert xml == expected

  job._toJDL(jobDescriptionObject=StringIO.StringIO(job._toXML()))


def test_SimpleParametricJob():

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

  assert job_id == '%(JOB_ID)s'
  assert inputData == '%(InputData)s'
  assert 'jobDescription.xml' in arguments
  assert '-o LogLevel=DEBUG' in arguments
  assert'-p JOB_ID=%(JOB_ID)s' in arguments
  assert'-p InputData=%(InputData)s' in arguments
