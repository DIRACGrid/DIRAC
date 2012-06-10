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
    Client.__init__( self )
    self.setServer( 'DataManagement/DataLogging' )
    if url:
      self.setServer( url )
    self.setTimeout( 120 )

def testDataLoggingClient():
  """ basic test of the module
  """
  import os
  import sys
  from DIRAC.Core.Base.Script import parseCommandLine
  from DIRAC import gLogger, S_OK
  parseCommandLine()
  gLogger.setLevel( 'VERBOSE' )

  if 'PYTHONOPTIMIZE' in os.environ and os.environ['PYTHONOPTIMIZE']:
    gLogger.info( 'Unset pyhthon optimization "PYTHONOPTIMIZE"' )
    sys.exit( 0 )

  gLogger.info( 'Testing DataLoggingClient class...' )

  try:

    result = S_OK()

    dlc = DataLoggingClient()

    gLogger.info( 'DataLoggingClient instantiated' )

    server = dlc.getServer()
    assert server == 'DataManagement/DataLogging'

    gLogger.info( ' Connecting to ', server )

    timeout = dlc.timeout
    assert timeout == 120

    result = dlc.ping()
    assert result['OK']

    gLogger.info( 'Server is alive' )

  except AssertionError, x:
    if result['OK']:
      gLogger.error( x )
      sys.exit( 1 )
    else:
      gLogger.info( 'Test OK, but could not connect to server' )
      gLogger.info( result['Message'] )

if __name__ == '__main__':
  testDataLoggingClient()
