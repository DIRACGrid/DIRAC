''' CEAvailabilityCommand module
'''
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

# FIXME: NOT Usable ATM
# missing doNew, doCache, doMaster

__RCSID__ = '$Id$'

from DIRAC import S_OK, S_ERROR
from DIRAC.ResourceStatusSystem.Command.Command import Command
from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOs


class CEAvailabilityCommand(Command):
  '''
    It returns the status of a given CE.
  '''

  def __init__(self, args=None):
    '''

      :params: args: dictionary specifying following values
        :attr:`ce`: Computing Element (mandatory)
        :attr:`host`: BDII server (optional)
    '''
    super(CEAvailabilityCommand, self).__init__(args)

  def doCommand(self):
    '''
      It returns the status of a given CE.

      :return:
        a dictionary with status of each CE queues,
        and 'status' and 'reason' of the CE itself

    '''

    # INPUT PARAMETERS
    vos = getVOs()
    if vos['OK']:
      vo = vos['Value'].pop()
    else:
      return S_ERROR("No appropriate VO was found! %s" % vos['Message'])

    if 'ce' not in self.args:
      return S_ERROR("No computing element 'ce' has been specified!")
    else:
      ce = self.args['ce']

    host = self.args.get('host')

    # getting BDII info
    diracAdmin = DiracAdmin()
    ceQueues = diracAdmin.getBDIICEState(ce,
                                         useVO=vo,
                                         host=host)
    if not ceQueues['OK']:
      return S_ERROR('"CE" not found on BDII')
    elements = ceQueues['Value']

    # extracting the list of CE queues and their status
    result = {}
    for element in elements:
      queue = element.get('GlueCEUniqueID', 'Unknown')  # pylint: disable=no-member
      statusQueue = element.get('GlueCEStateStatus', 'Unknown')  # pylint: disable=no-member
      result[queue] = statusQueue.capitalize()

    # establishing the status of the CE itself
    result['Status'] = 'Production'
    result['Reason'] = "All queues in 'Production'"
    for key, value in result.items():
      # warning: it may not be the case that all queues for a given CE
      # show the same status. In case of mismatch, the status of the CE
      # will be associated to a non-production status
      if key not in ['Status', 'Reason'] and value != 'Production':
        result['Status'] = value
        result['Reason'] = "Queue %s is in status %s" % (queue, value)

    return S_OK(result)

################################################################################
# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
