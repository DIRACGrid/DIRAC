########################################################################
# $HeadURL $
# File: LcgFileCatalogProxyClient.py
########################################################################
""" File catalog client for LCG File Catalog proxy service """
__RCSID__ = "$Id$"

from DIRAC.Core.Base.Client                     import Client

class LcgFileCatalogProxyClient( Client ):
  """ File catalog client for LCG File Catalog proxy service
  """

  def __init__( self, url = False, **kwargs ):
    """ Constructor of the LCGFileCatalogProxy client class
    """
    Client.__init__( self, **kwargs )
    self.method = None
    self.name = 'LFCProxy'
    self.valid = False
    self.setServer( 'DataManagement/LcgFileCatalogProxy' )
    if url:
      self.setServer( url )
    self.setTimeout( 120 )
    self.call = 'ping'
    self.valid = self.executeRPC()['OK']

  def isOK( self ):
    """ Is the Catalog available?
    """
    return self.valid

  def getName( self ):
    """ Get the file catalog type name
    """
    return self.name

  def __getattr__( self, name ):
    self.method = name
    return self.execute

  def execute( self, *parms, **kws ):
    """ Magic method dispatcher """
    self.call = 'callProxyMethod'
    return self.executeRPC( self.method, parms, kws )

def testLcgFileCatalogProxyClient():
  """ basic test of the module
  """
  import os
  import sys
  import pprint
  from DIRAC.Core.Base.Script import parseCommandLine
  from DIRAC import gLogger, S_OK
  parseCommandLine()
  gLogger.setLevel( 'VERBOSE' )

  if 'PYTHONOPTIMIZE' in os.environ and os.environ['PYTHONOPTIMIZE']:
    gLogger.info( 'Unset pyhthon optimization "PYTHONOPTIMIZE"' )
    sys.exit( 0 )

  gLogger.info( 'Testing LcgFileCatalogProxyClient class...' )

  try:

    result = S_OK()

    lfcpc = LcgFileCatalogProxyClient()

    gLogger.info( 'LcgFileCatalogProxyClient instantiated' )

    server = lfcpc.getServer()
    assert server == 'DataManagement/LcgFileCatalogProxy'

    gLogger.info( ' Connecting to ', server )

    timeout = lfcpc.timeout
    assert timeout == 120

    result = lfcpc.listDirectory( '/' )
    assert result['OK']

    gLogger.info( pprint.pformat( result['Value']['Successful'] ) )

    gLogger.info( 'Server is alive' )

  except AssertionError, x:
    if result['OK']:
      gLogger.error( x )
      sys.exit( 1 )
    else:
      gLogger.info( 'Test OK, but could not connect to server' )
      gLogger.info( result['Message'] )

if __name__ == '__main__':
  testLcgFileCatalogProxyClient()

