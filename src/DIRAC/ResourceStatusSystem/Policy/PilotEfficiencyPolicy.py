""" PilotEfficiencyPolicy

  Policy that gets efficiency from the PilotCommand result and
  sets the resource status. Efficiency is given in percent.

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

    efficiency < 50.0 :: Banned
    efficiency < 90.0 :: Degraded

    """
    # ideally,  this should be obtained from config.
    bannedLimit = 50.0
    degradedLimit = 90.0

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

    # Pilot efficiency is now available directly from the command result, in percent:
    efficiency = commandResult.get('PilotJobEff', None)
    # get the VO from the result, if present
    result['VO'] = commandResult.get('VO', None)

    if efficiency is None:
      result['Status'] = 'Unknown'
      result['Reason'] = 'Not enough pilots to take a decision'
      return S_OK(result)

    if efficiency <= bannedLimit:
      result['Status'] = 'Banned'
    elif efficiency <= degradedLimit:
      result['Status'] = 'Degraded'
    else:
      result['Status'] = 'Active'

    result['Reason'] = 'Pilots Efficiency of %.2f' % efficiency
    return S_OK(result)
