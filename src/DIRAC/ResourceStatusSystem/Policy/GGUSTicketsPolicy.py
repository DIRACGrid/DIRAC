""" policy that evaluates on how many tickets are open at the moment.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from DIRAC import S_OK
from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase

__RCSID__ = "$Id$"


class GGUSTicketsPolicy(PolicyBase):
  '''
  The GGUSTicketsPolicy class is a policy class that evaluates on
  how many tickets are open at the moment.

  GGUSTicketsPolicy, given the number of GGUS tickets opened, proposes a new
  status for the element.
  '''

  @staticmethod
  def _evaluate(commandResult):
    """
    Evaluate policy on opened tickets, using args (tuple).

    :returns:
        {
          'Status':Active|Probing,
          'Reason':'GGUSTickets: n unsolved',
        }
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

    # The command returns a list of dictionaries, with only one if thre is something,
    # otherwise an empty list.
    commandResult = commandResult[0]

    if 'OpenTickets' not in commandResult:
      result['Status'] = 'Error'
      result['Reason'] = 'Expected OpenTickets key for GGUSTickets'
      return S_OK(result)

    openTickets = commandResult['OpenTickets']

    if openTickets == 0:
      result['Status'] = 'Active'
      result['Reason'] = 'NO GGUSTickets unsolved'
    else:
      # Setting to Probing is way too aggresive, as we do not know the nature of the tickets
      result['Status'] = 'Degraded'
      result['Reason'] = '%s GGUSTickets unsolved: %s' % (openTickets, commandResult['Tickets'])

    return S_OK(result)
