""" Accounting class to stores network metrics gathered by perfSONARs.

    Filled by "Accounting/Network" agent
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

from DIRAC.AccountingSystem.Client.Types.BaseAccountingType import BaseAccountingType


class Network(BaseAccountingType):
  """
  Accounting type to stores network metrics gathered by perfSONARs.
  """

  def __init__(self):
    super(Network, self).__init__()

    # IPv6 address has up to 45 chars
    self.definitionKeyFields = [
        ('SourceIP', 'VARCHAR(50)'),
        ('DestinationIP', 'VARCHAR(50)'),
        ('SourceHostName', 'VARCHAR(50)'),
        ('DestinationHostName', 'VARCHAR(50)'),
        ('Source', 'VARCHAR(50)'),
        ('Destination', 'VARCHAR(50)')
    ]

    self.definitionAccountingFields = [
        ('Jitter', 'FLOAT'),
        ('OneWayDelay', 'FLOAT'),
        ('PacketLossRate', 'TINYINT UNSIGNED'),
    ]
    self.bucketsLength = [
        (86400 * 3, 900),  # <3d = 15m
        (86400 * 8, 3600),  # <1w+1d = 1h
        (15552000, 86400),  # >1w+1d <6m = 1d
        (31104000, 604800),  # >6m = 1w
    ]
    self.checkType()
