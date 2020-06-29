""" This tests only need the PilotAgentsDB, and connects directly to it

    Suggestion: for local testing, run this with::
        python -m pytest -c ../pytest.ini  -vv tests/Integration/WorkloadManagementSystem/Test_PilotAgentsDB.py
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

# pylint: disable=wrong-import-position

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC import gLogger
from DIRAC.WorkloadManagementSystem.DB.PilotAgentsDB import PilotAgentsDB

gLogger.setLevel('DEBUG')

paDB = PilotAgentsDB()


def test_basic():
  """ usual insert/verify
  """
  res = paDB.addPilotTQReference(['pilotRef'], 123, 'ownerDN', 'ownerGroup',)
  assert res['OK'] is True

  res = paDB.deletePilot('pilotRef')

  # FIXME: to expand...
