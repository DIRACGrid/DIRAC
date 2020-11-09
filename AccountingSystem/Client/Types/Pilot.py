from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
__RCSID__ = "$Id$"

from DIRAC.AccountingSystem.Client.Types.BaseAccountingType import BaseAccountingType


class Pilot(BaseAccountingType):

  def __init__(self):
    super(Pilot, self).__init__()
    self.definitionKeyFields = [('User', 'VARCHAR(64)'),
                                ('UserGroup', 'VARCHAR(32)'),
                                ('Site', 'VARCHAR(64)'),
                                ('GridCE', "VARCHAR(128)"),
                                ('GridMiddleware', 'VARCHAR(32)'),
                                ('GridResourceBroker', 'VARCHAR(128)'),
                                ('GridStatus', 'VARCHAR(32)'),
                                ]
    self.definitionAccountingFields = [('Jobs', "INT UNSIGNED"),
                                       ]
    self.checkType()
