
import time
import tempfile
import os
import DIRAC
from DIRAC import gLogger
from DIRAC.Core.Security import Locations, File
from DIRAC.Core.Security.X509Chain import X509Chain
from DIRAC.Core.Security.BaseSecurity import BaseSecurity

class MyProxy( BaseSecurity ):

  def getDelegatedProxy( self, proxyChain, lifeTime = 72 ):
    """
      Get delegated proxy from MyProxy server
      return S_OK( X509Chain ) / S_ERROR
    """
    #TODO: Set the proxy coming in proxyString to be the proxy to use

    #Get myproxy username diracgroup:diracuser
    retVal = proxyChain.getCredentials()
    if not retVal[ 'OK' ]:
      return retVal
    credDict = retVal[ 'Value' ]
    if not credDict[ 'isProxy' ]:
      return S_ERROR( "chain does not contain a proxy" )
    if not credDict[ 'validDN' ]:
      return S_ERROR( "DN %s is not known in dirac" % credDict[ 'subject' ] )
    if not credDict[ 'validGroup' ]:
      return S_ERROR( "Group %s is invalid for DN %s" % ( credDict[ 'group' ], credDict[ 'subject' ] ) )
    mpUsername = "%s:%s" % ( credDict[ 'group' ], credDict[ 'username' ] )

    #Dump chain to file
    retVal = File.writeChainToTemporaryFile( proxyChain )
    if not retVal[ 'OK' ]:
      return retVal
    baseProxyLocation = retVal[ 'Value' ]

    try:
      fd,newProxyLocation = tempfile.mkstemp()
      os.close(fd)
    except IOError:
      return S_ERROR('Failed to create temporary file for store proxy from MyProxy service')

    # myproxy-get-delegation works only with environment variables
    cmdEnv = self._getExternalCmdEnvironment()
    cmdEnv['X509_USER_PROXY'] = baseProxyLocation

    cmdArgs = []
    cmdArgs.append( "-s '%s'" % self._secServer )
    cmdArgs.append( "-t '%s'" % ( lifeTime + 1 ) )
    cmdArgs.append( "-a '%s'" % baseProxyLocation )
    cmdArgs.append( "-o '%s'" % newProxyLocation )
    cmdArgs.append( "-l '%s'" % mpUsername )

    cmd = "myproxy-get-delegation %s" % " ".join( cmdArgs )
    gLogger.verbose( "myproxy-get-delegation command:\n%s" % cmd )

    start = time.time()
    result = shellCall( self._secCmdTimeout, cmd, env = environment )
    query_time = time.time() - start

    self._unlinkFiles( baseProxyLocation )

    if not result['OK']:
      errMsg = "Call to myproxy-get-delegation failed: %s" % retVal[ 'Message' ]
      self._unlinkFiles( newProxyLocation )
      return S_ERROR( errMsg )

    status, output, error = result['Value']

    # Clean-up files
    if status:
      errMsg = "Call to myproxy-get-delegation failed"
      extErrMsg = 'Command: %s; StdOut: %s; StdErr: %s' % ( cmd, result, error )
      self._unlinkFiles( newProxyLocation )
      return S_ERROR( "%s %s" % ( errMsg, extErrMsg ) )

    chain = X509Chain()
    retVal = chain.loadProxyFromFile( newProxyLocation )
    if not retVal[ 'OK' ]:
      return S_ERROR( "myproxy-get-delegation failed when reading delegated file: %s" % retVal[ 'Message' ] )

    return S_OK( chain )