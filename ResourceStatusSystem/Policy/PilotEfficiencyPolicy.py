""" PilotEfficiencyPolicy

  Policy that calculates the efficiency following the formula::

    done / ( failed + aborted + done )

  if the denominator is smaller than 10, it does not take any decision.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from DIRAC import S_OK
from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase

__RCSID__ = '$Id$'


class PilotEfficiencyPolicy(PolicyBase):
  """ PilotEfficiencyPolicy class, extends PolicyBase
  """

  @staticmethod
  def _evaluate(commandResult):
    """ _evaluate

    efficiency < 0.5 :: Banned
    efficiency < 0.9 :: Degraded

    """

    result = {
        'Status': None,
        'Reason': None
    }

    if not commandResult['OK']:
      result['Status'] = 'Error'
      result['Reason'] = commandResult['Message']
      return S_OK(result)

    commandResult = commandResult['Value']

    if not commandResult:
      result['Status'] = 'Unknown'
      result['Reason'] = 'No values to take a decision'
      return S_OK(result)

    commandResult = commandResult[0]

    if not commandResult:
      result['Status'] = 'Unknown'
      result['Reason'] = 'No values to take a decision'
      return S_OK(result)

    aborted = commandResult['Aborted']
    # deleted = float( commandResult[ 'Deleted' ] )
    done = commandResult['Done']
    failed = commandResult['Failed']

    # total     = aborted + deleted + done + failed
    total = aborted + done + failed

    # we want a minimum amount of pilots to take a decision ( at least 10 pilots )
    if total < 10:
      result['Status'] = 'Unknown'
      result['Reason'] = 'Not enough pilots to take a decision'
      return S_OK(result)

    efficiency = done / total

    if efficiency <= 0.5:
      result['Status'] = 'Banned'
    elif efficiency <= 0.9:
      result['Status'] = 'Degraded'
    else:
      result['Status'] = 'Active'

    result['Reason'] = 'Pilots Efficiency of %.2f' % efficiency
    return S_OK(result)
