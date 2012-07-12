########################################################################
# $HeadURL $
# File: DataLoggingClient.py
########################################################################
""" :mod: DataLoggingClient 
    =======================
 
    .. module: DataLoggingClient
    :synopsis: client for DataLoggingDB
"""

## RSCID
__RCSID__ = "$Id$"

## imports
from DIRAC.Core.Base.Client import Client

class DataLoggingClient( Client ):
  """ 
  .. class:: DataLoggingClient

  rpc client for DataLoggingDB 
  """
  def __init__( self, url = "DataManagement/DataLogging" ):
    """ c'tor

    :param self: self reference
    :param str url: service URL
    """
    Client.__init__( self )
    self.setServer( url ) 
    self.setTimeout( 120 )

