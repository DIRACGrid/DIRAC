########################################################################
# $HeadURL$
########################################################################
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

__RCSID__ = "$Id$"

import os

from DIRAC                                               import gConfig, S_ERROR
from DIRAC.FrameworkSystem.Client.ProxyManagerClient     import gProxyManager
from DIRAC.ConfigurationSystem.Client.ConfigurationData  import gConfigurationData
from DIRAC.ConfigurationSystem.Client.Helpers.Registry   import getVOMSAttributeForGroup, getDNForUsername

def executeWithUserProxy( fcn ):
  """
  Decorator function to execute with a temporary user proxy

  :param fcn: function to be decorated
  :return: the result of the fcn execution

  In order to be executed with a user proxy, the function must be called with the
  following parameters:

  :param str proxyUserName: the user name of the proxy to be used
  :param str proxyUserGroup: the user group of the proxy to be used
  :param str proxyWithVOMS: optional flag to dress or not the user proxy with VOMS extension ( default True )
  :param str proxyFilePath: optional file location for the temporary proxy
  """

  def wrapped_fcn( *args, **kwargs ):

    userName = kwargs.pop( 'proxyUserName', '' )
    userGroup = kwargs.pop( 'proxyUserGroup', '' )
    vomsFlag = kwargs.pop( 'proxyWithVOMS', True )
    proxyFilePath = kwargs.pop( 'proxyFilePath', False )

    if userName and userGroup:

      # Setup user proxy
      originalUserProxy = os.environ.get( 'X509_USER_PROXY' )
      result = getDNForUsername( userName )
      if not result[ 'OK' ]:
        return result
      userDN = result[ 'Value' ][0]
      vomsAttr = ''
      if vomsFlag:
        vomsAttr = getVOMSAttributeForGroup( userGroup )

      if vomsAttr:
        result = gProxyManager.downloadVOMSProxyToFile( userDN, userGroup,
                                                        requiredVOMSAttribute = vomsAttr,
                                                        filePath = proxyFilePath,
                                                        requiredTimeLeft = 3600,
                                                        cacheTime =  3600 )
      else:
        result = gProxyManager.downloadProxyToFile( userDN, userGroup,
                                                    filePath = proxyFilePath,
                                                    requiredTimeLeft = 3600,
                                                    cacheTime =  3600 )

      if not result['OK']:
        return result

      proxyFile = result['Value']
      os.environ['X509_USER_PROXY'] = proxyFile

      # Check if the caller is executing with the host certificate
      useServerCertificate = gConfig.useServerCertificate()
      if useServerCertificate:
        gConfigurationData.setOptionInCFG( '/DIRAC/Security/UseServerCertificate', 'false' )

      try:
        resultFcn = fcn( *args, **kwargs )
      except Exception, x:
        resultFcn = S_ERROR( "Exception: %s" % str( x ) )

      # Restore the default host certificate usage if necessary
      if useServerCertificate:
          gConfigurationData.setOptionInCFG( '/DIRAC/Security/UseServerCertificate', 'true' )
      if originalUserProxy:
        os.environ['X509_USER_PROXY'] = originalUserProxy
      else:
        os.environ.pop( 'X509_USER_PROXY' )

      return resultFcn
    else:
      # No proxy substitution requested
      return fcn( *args, **kwargs )

  return wrapped_fcn
