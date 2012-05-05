########################################################################
# $HeadURL $
# File: DataLoggingClient.py
########################################################################
""" Client for DataLoggingDB
"""
__RCSID__ = "$Id$"

## imports
from DIRAC.Core.Base.Client             import Client


class DataLoggingClient( Client ):
  """ Client for DataLoggingDB
  """
  def __init__( self, url = None ):
    """ Constructor of the DataLogging client

    :param self: self reference
    :param str url: service URL
    :param useCertificates: flag to use certificates
    """
    self.setServer( 'DataManagement/DataLogging' )
    if url:
      self.setServer( url )
    self.setTimeout( 120 )
