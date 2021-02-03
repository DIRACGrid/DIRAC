########################################################################
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2012/12/11 18:04:25
########################################################################

""" :mod: SubprocessTests
    =======================

    .. module: SubprocessTests
    :synopsis: unittest for Subprocess module
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    unittest for Subprocess module
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

from os.path import dirname, join

# imports
import time
import pytest

from subprocess import Popen

# SUT
from DIRAC.Core.Utilities.Subprocess import systemCall, shellCall, pythonCall, getChildrenPIDs

# Mark this entire module as slow
pytestmark = pytest.mark.slow

cmd = ["sleep", "2"]


def pyfunc(_name):
  time.sleep(2)


@pytest.mark.parametrize("timeout, expected", [
    (False, True),
    (3, True),
    (1, False)
])
def test_calls(timeout, expected):
  ret = systemCall(timeout, cmdSeq=cmd)
  assert ret['OK'] == expected

  ret = shellCall(timeout, cmdSeq=" ".join(cmd))
  assert ret['OK'] == expected

  ret = pythonCall(timeout, pyfunc, 'something')
  assert ret['OK'] == expected


def test_getChildrenPIDs():
  import os
  os.system("echo $PWD")
  mainProcess = Popen(['python', join(dirname(__file__), 'ProcessesCreator.py')])
  time.sleep(1)
  res = getChildrenPIDs(mainProcess.pid)
  assert len(res) == 3
  for p in res:
    assert isinstance(p, int)

  mainProcess.wait()
