""" This integration test is for "Inner" Computing Element InProcessComputingElement
"""

from DIRAC.tests.Utilities.utils import find_all
from DIRAC import gLogger


# sut
from DIRAC.Resources.Computing.InProcessComputingElement import InProcessComputingElement

gLogger.setLevel('DEBUG')

# executable file
executableFile = 'hello.sh'


def test_submit():
  ce = InProcessComputingElement('InProcess')
  res = ce.submitJob(find_all(executableFile, 'tests')[0])
  assert res['OK'] is True
