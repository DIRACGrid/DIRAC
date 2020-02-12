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

__RCSID__ = "$Id$"

##
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/12/11 18:04:37

# imports
import time
import pytest
# SUT
from DIRAC.Core.Utilities.Subprocess import systemCall, shellCall, pythonCall

########################################################################

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
