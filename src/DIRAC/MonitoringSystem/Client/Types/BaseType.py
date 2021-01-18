"""
Helper class for configuring the monitoring service.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


__RCSID__ = "$Id$"

########################################################################


class BaseType(object):

  """
  .. class:: BaseType

  :param str index: we use daily indexes for example:wmshistory_index-2015-10-09
  :param keyFields: The attributes what we monitor.
  :type keyFields: python:list
  :param monitoringFields: This is the value what we plot
  :type monitoringFields: python:list
  :param int dataToKeep: Data retention. We keep all data by default.
  :param dict mapping: We can specify the mapping of the documents. It is used during the creation of an index.
                       Note: If you do not want to be analysed a string, you have to set the mapping
  :param str period: We can configure the elasticsearch index name with a period.
                     By default we use daily indexes, but we can also use weekly, monthly, yearly indexes.
                     Or use no period at all.
                     Supported values: day, week, month, year, null

  """

  ########################################################################
  def __init__(self):
    """ c'tor

    :param self: self reference
    """
    self.keyFields = []
    self.mapping = {}  # by default we do not have mapping
    self.timeType = {'properties': {'timestamp': {'type': 'date'}}}  # we use timestamp for all monitoring types.
    self.period = 'day'
    self.monitoringFields = ["Value"]
    self.index = None
    self.index = self._getIndex()

    # we only keep the last month of the data.
    self.dataToKeep = -1

  ########################################################################
  def checkType(self):
    """
    The mandatory fields has to be present
    """
    if not self.keyFields:
      raise Exception("keyFields has to be provided!")
    if not self.monitoringFields:
      raise Exception("monitoringFields has to be provided!")

  ########################################################################
  def _getIndex(self):
    """It returns and index based on the name of the type.
    For example: WMSMonitorType the type the index will be wmsmonitor
    """
    index = ''
    if self.index is None:
      fullName = self.__class__.__name__
      index = "%s-index" % fullName.lower()
    else:
      index = self.index
    return index

  ########################################################################
  def addMapping(self, mapping):
    """
    :param dict mapping: the mapping used by elasticsearch
    """
    self.mapping = self.timeType
    self.mapping['properties'].update(mapping)
