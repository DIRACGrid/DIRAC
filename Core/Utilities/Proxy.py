"""
Utilities to execute a function with a given proxy.

executeWithUserProxy decorator example usage::

  @executeWithUserProxy
  def testFcn( x, i, kw = 'qwerty' ):

    print "args", x, i
    print "kwargs", kw
    print os.environ.get( 'X509_USER_PROXY' )
    return S_OK()

  ...

  result = testFcn( 1.0, 1, kw = 'asdfghj', proxyUserName = 'atsareg', proxyUserGroup = 'biomed_user' )

"""

import os

from DIRAC                                               import gConfig, gLogger, S_ERROR
from DIRAC.FrameworkSystem.Client.ProxyManagerClient     import gProxyManager
from DIRAC.ConfigurationSystem.Client.ConfigurationData  import gConfigurationData
from DIRAC.ConfigurationSystem.Client.Helpers.Registry   import getVOMSAttributeForGroup, getDNForUsername

__RCSID__ = "$Id$"

def executeWithUserProxy( fcn ):
  """
  Decorator function to execute with a temporary user proxy

  :param fcn: function to be decorated
  :return: the result of the fcn execution

  In order to be executed with a user proxy, the function must be called with the
  following parameters:

  :param str proxyUserName: the user name of the proxy to be used
  :param str proxyUserGroup: the user group of the proxy to be used
  :param str proxyUserDN: the user DN of the proxy to be used
  :param str proxyWithVOMS: optional flag to dress or not the user proxy with VOMS extension ( default True )
  :param str proxyFilePath: optional file location for the temporary proxy
  """

  def wrapped_fcn( *args, **kwargs ):

    userName = kwargs.pop( 'proxyUserName', '' )
    userDN = kwargs.pop( 'proxyUserDN', '' )
    userGroup = kwargs.pop( 'proxyUserGroup', '' )
    vomsFlag = kwargs.pop( 'proxyWithVOMS', True )
    proxyFilePath = kwargs.pop( 'proxyFilePath', False )

    if ( userName or userDN ) and userGroup:

      # Setup user proxy
      originalUserProxy = os.environ.get( 'X509_USER_PROXY' )
      if userDN:
        userDNs = [userDN]
      else:
        result = getDNForUsername( userName )
        if not result[ 'OK' ]:
          return result
        userDNs = result['Value'] # a same user may have more than one DN
      vomsAttr = ''
      if vomsFlag:
        vomsAttr = getVOMSAttributeForGroup( userGroup )

      result = getProxy(userDNs, userGroup, vomsAttr, proxyFilePath)

      if not result['OK']:
        return result

      proxyFile = result['Value']
      os.environ['X509_USER_PROXY'] = proxyFile

      # Check if the caller is executing with the host certificate
      useServerCertificate = gConfig.useServerCertificate()
      if useServerCertificate:
        gConfigurationData.setOptionInCFG( '/DIRAC/Security/UseServerCertificate', 'false' )

      try:
        return fcn( *args, **kwargs )
      except Exception as lException: #pylint: disable=broad-except
        value = ','.join( [str( arg ) for arg in lException.args] )
        exceptType = lException.__class__.__name__
        return S_ERROR( "Exception - %s: %s" % ( exceptType, value ) )
      finally:
        # Restore the default host certificate usage if necessary
        if useServerCertificate:
          gConfigurationData.setOptionInCFG( '/DIRAC/Security/UseServerCertificate', 'true' )
        if originalUserProxy:
          os.environ['X509_USER_PROXY'] = originalUserProxy
        else:
          os.environ.pop( 'X509_USER_PROXY' )

    else:
      # No proxy substitution requested
      return fcn( *args, **kwargs )

  return wrapped_fcn


def getProxy(userDNs, userGroup, vomsAttr, proxyFilePath):
  """ do the actual download of the proxy, trying the different DNs
  """
  for userDN in userDNs:
    if vomsAttr:
      result = gProxyManager.downloadVOMSProxyToFile( userDN, userGroup,
                                                      requiredVOMSAttribute = vomsAttr,
                                                      filePath = proxyFilePath,
                                                      requiredTimeLeft = 3600,
                                                      cacheTime = 3600 )
    else:
      result = gProxyManager.downloadProxyToFile( userDN, userGroup,
                                                  filePath = proxyFilePath,
                                                  requiredTimeLeft = 3600,
                                                  cacheTime = 3600 )

    if not result['OK']:
      gLogger.warn( "Can't download proxy of '%s' to file" %userDN, result['Message'] )
    else:
      return result

    return S_ERROR("Can't download proxy")
