"""
Helper class for configuring the monitoring service.
"""

from DIRAC import gLogger

__RCSID__ = "$Id$"

########################################################################
class BaseType( object ):

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
  :param str period: We can configure the elasticsearch index type. By default we use daily indexes. But we can use monthly indexes.
                     Supported values: day, month
  
  """

  __doc_type = None
  __index = None
  __keyFields = []
  __monitoringFields = []
  __dataToKeep = None
  __mapping = {'time_type':{'properties' : {'timestamp': {'type': 'date'} } } } #we use timestamp for all monitoring types.
  __period = 'day'

  ########################################################################
  def __init__( self ):
    """ c'tor
    :param self: self reference
    """

    self.__monitoringFields = ["Value"]
    self.__index = self._getIndex()

    # we only keep the last month of the data.
    self.__dataToKeep = -1

  ########################################################################
  def checkType( self ):
    """
    The mandatory fields has to be present
    """
    if not self.__keyFields:
      raise Exception( "keyFields has to be provided!" )
    if not self.__monitoringFields:
      raise Exception( "monitoringFields has to be provided!" )

  ########################################################################
  def _getIndex ( self ):
    """It returns and index based on the name of the type.
    For example: WMSMonitorType the type the index will be wmsmonitor
    """
    index = ''
    if self.__index is None:
      fullName = self.__class__.__name__
      index = "%s-index" % fullName.lower()
    else:
      index = self.__index
    return index

  ########################################################################
  def _getDocType( self ):
    """
    It returns the corresponding category. The type of a document.
    """
    doctype = ''
    if self.__doc_type is None:
      fullName = self.__class__.__name__
      doctype = fullName
    else:
      doctype = self.__doc_type
    return doctype


  ########################################################################
  def getDataToKeep( self ):
    """
    returns the interval
    """
    return self.__dataToKeep
  
  ########################################################################
  def getKeyFields( self ):
    """
    it return the list of the fields what we monitor
    """
    return self.__keyFields

  ########################################################################
  def getMonitoringFields( self ):
    """
    It returns the attributes which will be plotted
    """
    return self.__monitoringFields

  ########################################################################
  def addMapping(self, mapping):
    """
    :param dict mapping: the mapping used by elasticsearch
    """
    self.__mapping.update(mapping)

  ########################################################################
  def getMapping(self):
    """
    It returns a specific mapping, which is used by a certain monitoring type 
    """
    return self.__mapping

  ########################################################################
  def getPeriod( self ):
    """
    
    It returns the indexing period.
    
    """
    return self.__period
