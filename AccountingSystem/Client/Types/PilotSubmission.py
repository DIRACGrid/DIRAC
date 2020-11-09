""" Accounting Type for Pilot Submission

    Filled by the "WorkloadManagement/SiteDirector" agent(s)
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


from DIRAC.AccountingSystem.Client.Types.BaseAccountingType import BaseAccountingType

__RCSID__ = "$Id$"


class PilotSubmission(BaseAccountingType):
  """ Accounting Type class for Pilot Submission
  """

  def __init__(self):
    super(PilotSubmission, self).__init__()

    self.definitionKeyFields = [('HostName', 'VARCHAR(100)'),
                                ('SiteDirector', 'VARCHAR(100)'),
                                ('Site', 'VARCHAR(100)'),
                                ('CE', 'VARCHAR(100)'),
                                ('Queue', 'VARCHAR(100)'),
                                ('Status', 'VARCHAR(100)')]
    self.definitionAccountingFields = [('NumTotal', "INT UNSIGNED"),
                                       ('NumSucceeded', 'INT UNSIGNED')]

    self.bucketsLength = [(86400 * 2, 900),         # <2d = 15m
                          (86400 * 10, 9000),       # <10d = 2.5h
                          (86400 * 35, 18000),      # <35d = 5h
                          (86400 * 30 * 6, 86400),  # >5d <6m = 1d
                          (86400 * 600, 604800)]    # >6m = 1w

    self.dataTimespan = 86400 * 30 * 14  # Only keep the last 14 months of data
    self.checkType()
