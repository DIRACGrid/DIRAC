
import time
import tempfile
import os
import DIRAC
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.Subprocess import shellCall
import DIRAC.Core.Security.Locations as Locations
import DIRAC.Core.Security.File as File
from DIRAC.Core.Security.X509Chain import X509Chain
from DIRAC.Core.Security.BaseSecurity import BaseSecurity
from DIRAC.Core.Security.X509Chain import g_X509ChainType

class MyProxy( BaseSecurity ):

  def uploadProxy( self, proxy = False, useDNAsUserName = False ):
    """
    Upload a proxy to myproxy service.
      proxy param can be:
        : Default -> use current proxy
        : string -> upload file specified as proxy
        : X509Chain -> use chain
    """
    retVal = self._loadProxy( proxy )
    if not retVal[ 'OK' ]:
      return retVal
    proxyDict = retVal[ 'Value' ]
    chain = proxyDict[ 'chain' ]
    proxyLocation = proxyDict[ 'file' ]

    retVal = self._getUsername( chain )
    if not retVal[ 'OK' ]:
      if proxyDict[ 'tempFile' ]:
        self._unlinkFiles( proxyLocation )
      return retVal
    mpUsername = retVal[ 'Value' ]

    timeLeft = int( chain.getRemainingSecs()[ 'Value' ] / 3600 )

    cmdArgs = []
    cmdArgs.append( '-s "%s"' % self._secServer )
    cmdArgs.append( '-c "%s"' % ( timeLeft - 5 ) )
    cmdArgs.append( '-C "%s"' % proxyLocation )
    cmdArgs.append( '-y "%s"' % proxyLocation )
    if useDNAsUserName:
      cmdArgs.append( '-d' )
    else:
      cmdArgs.append( '-l "%s"' % mpUsername )

    cmd = "myproxy-init %s" % " ".join( cmdArgs )
    result = shellCall( self._secCmdTimeout, cmd, env = self._getExternalCmdEnvironment( noX509 = True ) )

    if proxyDict[ 'tempFile' ]:
        self._unlinkFiles( proxyLocation )

    if not result['OK']:
      errMsg = "Call to myproxy-init failed: %s" % retVal[ 'Message' ]
      return S_ERROR( errMsg )

    status, output, error = result['Value']

    # Clean-up files
    if status:
      errMsg = "Call to myproxy-init failed"
      extErrMsg = 'Command: %s; StdOut: %s; StdErr: %s' % ( cmd, result, error )
      return S_ERROR( "%s %s" % ( errMsg, extErrMsg ) )

    return S_OK()

  def getDelegatedProxy( self, proxyChain, lifeTime = 72, useDNAsUserName = False ):
    """
      Get delegated proxy from MyProxy server
      return S_OK( X509Chain ) / S_ERROR
    """
    #TODO: Set the proxy coming in proxyString to be the proxy to use

    #Get myproxy username diracgroup:diracuser
    retVal = self._loadProxy( proxy )
    if not retVal[ 'OK' ]:
      return retVal
    proxyDict = retVal[ 'Value' ]
    chain = proxyDict[ 'chain' ]
    proxyLocation = proxyDict[ 'file' ]

    retVal = self._getUsername( chain )
    if not retVal[ 'OK' ]:
      if proxyDict[ 'tempFile' ]:
        self._unlinkFiles( proxyLocation )
      return retVal
    mpUsername = retVal[ 'Value' ]

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
    cmdArgs.append( "-a '%s'" % proxyLocation )
    cmdArgs.append( "-o '%s'" % newProxyLocation )
    if useDNAsUserName:
      cmdArgs.append( '-d' )
    else:
      cmdArgs.append( '-l "%s"' % mpUsername )

    cmd = "myproxy-get-delegation %s" % " ".join( cmdArgs )
    gLogger.verbose( "myproxy-get-delegation command:\n%s" % cmd )

    result = shellCall( self._secCmdTimeout, cmd, env = environment )

    if proxyDict[ 'tempFile' ]:
        self._unlinkFiles( proxyLocation )

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
