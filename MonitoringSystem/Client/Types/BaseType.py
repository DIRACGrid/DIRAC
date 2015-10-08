########################################################################
# $Id: $
########################################################################

"""
Helper class for configuring the monitoring service. 

"""

__RCSID__ = "$Id$"

########################################################################
class BaseType( object ):
  
  """
  .. class:: BaseType

  :param str doc_type: Each document belong to a category. For example: WMSHistory
  :param str index: we use daily indexes for example:wmshistory_index-2015-10-09 
  :param list keyFields: The attributes what we monitor.
  :param list monitoringFields: This is the value what we plot
  :param int dataToKeep: Data retention. We keep all data by default.
  """
  
  __doc_type = None
  __index = None
  __keyFields = []
  __monitoringFields = []
  __dataToKeep = None
  
  ########################################################################
  def __init__( self ):
    """ c'tor
    :param self: self reference
    """
        
    self.__monitoringFields = ["Value"]
    self.__index = self.getIndex()
    
    # we only keep the last month of the data.
    self.__dataToKeep = -1 
  
  ########################################################################  
  def checkType( self ):
    """
    The mandatory fields has to be present
    """
    if len( self.__keyFields ) == 0:
      raise Exception( "keyFields has to be provided!" )
    if len( self.__monitoringFields ) == 0:
      raise Exception( "monitoringFields has to be provided!" )

  ########################################################################
  def getIndex ( self ):
    """It returns and index based on the name of the type.
    For example: WMSMonitorType the type the index will be wmsmonitor
    """
    index = ''
    if self.__index == None:
      fullName = self.__class__.__name__
      index = "%s-index" % fullName
    else:
      index = self.__index
    return index
  
  ########################################################################
  def setIndex( self, name ):
    """
    Set the index name
    :param str name: the name of the index
    """
    self.__index = name
    
  ########################################################################
  def getDocType( self ):
    """
    It returns the corresponding category. The type of a document.
    """
    doctype = ''
    if self.__doc_type == None:
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
  def setDataToKeep( self, seconds ):
    """
    Set the period 
    :param int seconds : It keeps seconds data only.
    """
    self.__dataToKeep = seconds

  ########################################################################
  def setKeyFields( self, fields ):
    """
    :param list fields: it is a list of attributes
    """ 
    self.__keyFields = fields
  
  ########################################################################
  def getKeyFields( self ):
    """
    it return the list of the fields what we monitor
    """
    return self.__keyFields
  
  ########################################################################
  def setMonitoringFields( self, fields ):
    """
    :param list fields: list of attributes what we plot
    """
    self.__monitoringFields = fields
  
  ########################################################################
  def getMonitoringFields( self ):
    """
    It returns the attributes which will be plotted
    """
    return self.__monitoringFields
