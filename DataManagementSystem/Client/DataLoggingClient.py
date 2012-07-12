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

  client for DataLoggingDB 
  """
  def __init__( self, url = None ):
    """ Constructor of the DataLogging client

    :param self: self reference
    :param str url: service URL
    """
    Client.__init__( self )
    rec = self.setServer( url ) if url else self.setServer( "DataManagement/DataLogging" )
    self.setTimeout( 120 )

