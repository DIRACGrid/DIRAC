"""
Utilities to execute one or more functions with a given proxy.

:func:`executeWithUserProxy` decorator example usage::

  @executeWithUserProxy
  def testFcn(x, i, kw='qwerty'):

    print "args", x, i
    print "kwargs", kw
    print os.environ.get('X509_USER_PROXY')
    return S_OK()

  ...

  result = testFcn( 1.0, 1, kw = 'asdfghj', proxyUserName = 'atsareg', proxyUserGroup = 'biomed_user' )


:func:`executeWithUserProxy` function wrapper example::

  def undecoratedFunction(foo='bar'):
     print foo, os.environ.get('X509_USER_PROXY')
     return S_OK()

  executeWithUserProxy(testFcn)(foo='baz', proxyUserName='atsareg', proxyUserGroup='biomed_user')


:class:`UserProxy` context manager example::

  with UserProxy(proxyUserName='user', proxyUserGroup='group') as proxyResult:
    if proxyResult['OK']:
      functionThatNeedsAProxy()
      anotherFunction()


"""
import os

from DIRAC import gConfig, gLogger, S_ERROR, S_OK
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOMSAttributeForGroup, getDNForUsername
from DIRAC.Core.Utilities.LockRing import LockRing


def executeWithUserProxy(fcn):
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
    :param bool executionLock: flag to execute with a lock for the time of user proxy application ( default False )
    """

    def wrapped_fcn(*args, **kwargs):

        userName = kwargs.pop("proxyUserName", "")
        userDN = kwargs.pop("proxyUserDN", "")
        userGroup = kwargs.pop("proxyUserGroup", "")
        vomsFlag = kwargs.pop("proxyWithVOMS", True)
        proxyFilePath = kwargs.pop("proxyFilePath", False)
        executionLockFlag = kwargs.pop("executionLock", False)

        if (userName or userDN) and userGroup:

            proxyResults = _putProxy(
                userName=userName,
                userDN=userDN,
                userGroup=userGroup,
                vomsFlag=vomsFlag,
                proxyFilePath=proxyFilePath,
                executionLockFlag=executionLockFlag,
            )
            if not proxyResults["OK"]:
                return proxyResults
            originalUserProxy, useServerCertificate, executionLock = proxyResults["Value"]

            try:
                return fcn(*args, **kwargs)
            except Exception as lException:  # pylint: disable=broad-except
                value = ",".join([str(arg) for arg in lException.args])
                exceptType = lException.__class__.__name__
                return S_ERROR(f"Exception - {exceptType}: {value}")
            finally:
                _restoreProxyState(originalUserProxy, useServerCertificate, executionLock)
        else:
            # No proxy substitution requested
            return fcn(*args, **kwargs)

    return wrapped_fcn


def getProxy(userDNs, userGroup, vomsAttr, proxyFilePath):
    """do the actual download of the proxy, trying the different DNs"""
    for userDN in userDNs:
        if vomsAttr:
            result = gProxyManager.downloadVOMSProxyToFile(
                userDN,
                userGroup,
                requiredVOMSAttribute=vomsAttr,
                filePath=proxyFilePath,
                requiredTimeLeft=3600,
                cacheTime=3600,
            )
        else:
            result = gProxyManager.downloadProxyToFile(
                userDN, userGroup, filePath=proxyFilePath, requiredTimeLeft=3600, cacheTime=3600
            )

        if not result["OK"]:
            gLogger.error(
                "Can't download %sproxy " % ("VOMS" if vomsAttr else ""),
                f"of '{userDN}', group {userGroup} to file: " + result["Message"],
            )
        else:
            return result

    # If proxy not found for any DN, return an error
    return S_ERROR("Can't download proxy")


def executeWithoutServerCertificate(fcn):
    """
    Decorator function to execute a call without the server certificate.
    This shows useful in Agents when we want to call a DIRAC service
    and use the shifter proxy (for example Write calls to the DFC).

    The method does not fetch any proxy, it assumes it is already
    set up in the environment.
    Note that because it modifies the configuration for all thread,
    it uses a lock (the same as ExecuteWithUserProxy)

    Potential problem:
      * there is a lock for this particular method, but any other method
        changing the UseServerCertificate value can clash with this.

    :param fcn: function to be decorated
    :return: the result of the fcn execution

    """

    def wrapped_fcn(*args, **kwargs):

        # Get the lock and acquire it
        executionLock = LockRing().getLock("_UseUserProxy_", recursive=True)
        executionLock.acquire()

        # Check if the caller is executing with the host certificate
        useServerCertificate = gConfig.useServerCertificate()
        if useServerCertificate:
            gConfigurationData.setOptionInCFG("/DIRAC/Security/UseServerCertificate", "false")

        try:
            return fcn(*args, **kwargs)
        except Exception as lException:  # pylint: disable=broad-except
            value = ",".join([str(arg) for arg in lException.args])
            exceptType = lException.__class__.__name__
            return S_ERROR(f"Exception - {exceptType}: {value}")
        finally:
            # Restore the default host certificate usage if necessary
            if useServerCertificate:
                gConfigurationData.setOptionInCFG("/DIRAC/Security/UseServerCertificate", "true")
            # release the lock
            executionLock.release()

    return wrapped_fcn


class UserProxy:
    """Implement a context manager to execute functions with a user proxy."""

    def __init__(
        self,
        proxyUserName=None,
        proxyUserGroup=None,
        proxyUserDN=None,
        proxyWithVOMS=True,
        proxyFilePath=None,
        executionLock=False,
    ):
        """Construct the context manager for a user proxy.

        :param str proxyUserName: the user name of the proxy to be used
        :param str proxyUserGroup: the user group of the proxy to be used
        :param str proxyUserDN: the user DN of the proxy to be used
        :param bool proxyWithVOMS: optional flag to dress or not the user proxy with VOMS extension ( default True )
        :param str proxyFilePath: optional file location for the temporary proxy
        :param bool executionLock: flag to execute with a lock for the time of user proxy application ( default False )
        """
        if not ((proxyUserName or proxyUserDN) and proxyUserGroup):
            self.result = S_OK()
        else:
            self.result = _putProxy(
                userDN=proxyUserDN,
                userName=proxyUserName,
                userGroup=proxyUserGroup,
                vomsFlag=proxyWithVOMS,
                executionLockFlag=executionLock,
                proxyFilePath=proxyFilePath,
            )

    def __enter__(self):
        return self.result

    def __exit__(self, _type, _value, _traceback):
        """Reset the proxy and certificate state at the end of the managed block."""
        if self.result["OK"] and self.result["Value"]:
            originalUserProxy, useServerCertificate, executionLock = self.result["Value"]
            _restoreProxyState(originalUserProxy, useServerCertificate, executionLock)


def _putProxy(userDN=None, userName=None, userGroup=None, vomsFlag=None, proxyFilePath=None, executionLockFlag=False):
    """Download proxy, place in a file and populate X509_USER_PROXY environment variable.

    Parameters like `userProxy` or `executeWithUserProxy`.
    :returns: Tuple of originalUserProxy, useServerCertificate, executionLock
    """
    # Setup user proxy
    if userDN:
        userDNs = [userDN]
    else:
        result = getDNForUsername(userName)
        if not result["OK"]:
            return result
        userDNs = result["Value"]  # a same user may have more than one DN

    vomsAttr = ""
    if vomsFlag:
        vomsAttr = getVOMSAttributeForGroup(userGroup)

    result = getProxy(userDNs, userGroup, vomsAttr, proxyFilePath)

    if not result["OK"]:
        return result

    executionLock = LockRing().getLock("_UseUserProxy_", recursive=True) if executionLockFlag else None
    if executionLockFlag:
        executionLock.acquire()

    os.environ["X509_USER_PROXY"], originalUserProxy = result["Value"], os.environ.get("X509_USER_PROXY")

    # Check if the caller is executing with the host certificate
    useServerCertificate = gConfig.useServerCertificate()
    if useServerCertificate:
        gConfigurationData.setOptionInCFG("/DIRAC/Security/UseServerCertificate", "false")

    return S_OK((originalUserProxy, useServerCertificate, executionLock))


def _restoreProxyState(originalUserProxy=None, useServerCertificate=None, executionLock=None):
    """Restore the default host certificate and proxy state if necessary.

    Parameters like the output from `_putProxy`.
    """
    if useServerCertificate:
        gConfigurationData.setOptionInCFG("/DIRAC/Security/UseServerCertificate", "true")
    if originalUserProxy:
        os.environ["X509_USER_PROXY"] = originalUserProxy
    else:
        os.environ.pop("X509_USER_PROXY", None)
    if executionLock:
        executionLock.release()
