""" This integration test is for "Inner" Computing Element SingularityComputingElement
"""

from DIRAC.tests.Utilities.utils import find_all
from DIRAC import gLogger


# sut
from DIRAC.Resources.Computing.SingularityComputingElement import SingularityComputingElement

gLogger.setLevel('DEBUG')

# executable file
executableFile = 'hello.sh'


def test_submit():
  jobDesc = {"jobID": 123,
             "jobParams": {},
             "resourceParams": {},
             "optimizerParams": {}}

  ce = SingularityComputingElement('InProcess')

  res = ce.submitJob(find_all(executableFile, 'tests')[0],
                     jobDesc=jobDesc,
                     log=gLogger.getSubLogger('job_log'),
                     logLevel='DEBUG')
  assert res['OK'] is True
