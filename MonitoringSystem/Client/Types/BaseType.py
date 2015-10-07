########################################################################
# $Id: $
########################################################################

"""
Helper class for configuring the monitoring service. 

"""

__RCSID__ = "$Id$"

class BaseType( object ):
  
  def __init__( self ):
    """
    The default is the Value
    """
    
    self.index = None
    
    self.keyFields = []
    
    self.monitoringFields = ["Value"]
    
    # we only keep the last month of the data.
    self.dataToKeep = 86400 * 30
    
  def checkType( self ):
    """
    The mandatory fields has to be present
    """
    if len( self.keyFields ) == 0:
      raise Exception( "keyFields has to be provided!" )
    if len( self.monitoringFields ) == 0:
      raise Exception( "monitoringFields has to be provided!" )

  def getIndex (self):
    index = ''
    if self.index == None:
      fullName = self.__class__.__name__
      index = fullName.replace("Type", "")
    else:
      index = self.index
    return index