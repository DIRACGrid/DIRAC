""" CEAvailabilityPolicy module
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = '$Id$'

from DIRAC import S_OK
from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase


class CEAvailabilityPolicy(PolicyBase):
  """
    The CEAvailabilityPolicy checks if the CE is in 'Production' or not on the BDII.
  """

  @staticmethod
  def _evaluate(commandResult):
    """
      It returns Active status if CE is in 'Production'.
      Banned if the CE is different from 'Production'.

      commandResult is a dictionary like:
        {'OK': True,
        'Value': {
          'Reason': "All queues in 'Production'",
          'Status': 'Production',
          'cccreamceli05.in2p3.fr:8443/cream-sge-long': 'Production',
          'cccreamceli05.in2p3.fr:8443/cream-sge-verylong': 'Production'
          }
        }

      Otherwise, it returns error.
    """

    result = {'Status': None,
              'Reason': None}

    if not commandResult['OK']:
      result['Status'] = 'Error'
      result['Reason'] = commandResult['Message']
      return S_OK(result)

    commandResult = commandResult['Value']

    if commandResult['Status'] == 'Production':
      result['Status'] = 'Active'
    else:
      result['Status'] = 'Banned'

    result['Reason'] = commandResult['Reason']

    return S_OK(result)
