""" AlwaysActivePolicy module
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = '$Id$'

from DIRAC import S_OK
from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase


class AlwaysActivePolicy(PolicyBase):
  """
    The AlwaysActivePolicy is a dummy module that can be used as example, it
    always returns Active status.
  """

  @staticmethod
  def _evaluate(commandResult):
    """
      It returns Active status, evaluates the default command, but its output
      is completely ignored.
    """

    policyResult = {'Status': 'Active',
                    'Reason': 'AlwaysActive'}

    return S_OK(policyResult)
