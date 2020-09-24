"""
Helper class for configuring the monitoring service.
"""


__RCSID__ = "$Id$"

########################################################################


class BaseType(object):

  """
  .. class:: BaseType

  :param str doc_type: Each document belong to a category. For example: WMSHistory
  :param str index: we use daily indexes for example:wmshistory_index-2015-10-09
  :param keyFields: The attributes what we monitor.
  :type keyFields: python:list
  :param monitoringFields: This is the value what we plot
  :type monitoringFields: python:list
  :param int dataToKeep: Data retention. We keep all data by default.
  :param dict mapping: We can specify the mapping of the documents. It is used during the creation of an index.
                       Note: If you do not want to be analysed a string, you have to set the mapping
  :param str period: We can configure the elasticsearch index type.
                     By default we use daily indexes, but we can use monthly indexes.
                     Supported values: day, month

  """

  ########################################################################
  def __init__(self):
    """ c'tor

    :param self: self reference
    """
    self.doc_type = None
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
  def _getDocType(self):
    """
    It returns the corresponding category. The type of a document.
    """
    doctype = ''
    if self.doc_type is None:
      fullName = self.__class__.__name__
      doctype = fullName
    else:
      doctype = self.doc_type
    return doctype

  ########################################################################

  def addMapping(self, mapping):
    """
    :param dict mapping: the mapping used by elasticsearch
    """
    docType = self._getDocType()
    self.mapping.setdefault(docType, self.timeType)
    self.mapping[docType]['properties'].update(mapping)
