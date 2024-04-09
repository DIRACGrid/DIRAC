""" Collection of utilities for dealing with security files (i.e. proxy files)
"""
import os
import tempfile

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities import DErrno
from DIRAC.Core.Security.DiracX import addTokenToPEM
from DIRAC.Core.Utilities.File import secureOpenForWrite
from DIRAC.Core.Security.X509Chain import X509Chain  # pylint: disable=import-error
from DIRAC.Core.Security.Locations import getProxyLocation


def writeToProxyFile(proxyContents, fileName=False):
    """Write a proxy string to file

    arguments:
      - proxyContents : string object to dump to file
      - fileName : filename to dump to
    """
    try:
        with secureOpenForWrite(fileName) as (fd, fileName):
            fd.write(proxyContents)
    except Exception as e:
        return S_ERROR(DErrno.EWF, f" {fileName}: {repr(e).replace(',)', ')')}")

    # Add DiracX token to the file
    proxy = X509Chain()
    retVal = proxy.loadProxyFromFile(fileName)
    if not retVal["OK"]:
        return S_ERROR(DErrno.EPROXYREAD, f"ProxyLocation: {fileName}")
    retVal = proxy.getDIRACGroup(ignoreDefault=True)
    if not retVal["OK"]:
        return S_ERROR(DErrno.EPROXYREAD, f"No DIRAC group found in proxy: {fileName}")
    retVal = addTokenToPEM(fileName, retVal["Value"])  # pylint: disable=unsubscriptable-object
    if not retVal["OK"]:  # pylint: disable=unsubscriptable-object
        return retVal

    return S_OK(fileName)


def writeChainToProxyFile(proxyChain, fileName):
    """
    Write an X509Chain to file

    arguments:
      - proxyChain : X509Chain object to dump to file
      - fileName : filename to dump to
    """
    retVal = proxyChain.dumpAllToString()
    if not retVal["OK"]:
        return retVal
    return writeToProxyFile(retVal["Value"], fileName)


def writeChainToTemporaryFile(proxyChain):
    """
    Write a proxy chain to a temporary file
    return S_OK( string with name of file )/ S_ERROR
    """
    try:
        fd, proxyLocation = tempfile.mkstemp()
        os.close(fd)
    except OSError:
        return S_ERROR(DErrno.ECTMPF)
    retVal = writeChainToProxyFile(proxyChain, proxyLocation)
    if not retVal["OK"]:
        try:
            os.unlink(proxyLocation)
        except Exception:
            pass
        return retVal
    return S_OK(proxyLocation)


def deleteMultiProxy(multiProxyDict):
    """
    Delete a file from a multiProxyArgument if needed
    """
    if multiProxyDict["tempFile"]:
        try:
            os.unlink(multiProxyDict["file"])
        except Exception:
            pass


def multiProxyArgument(proxy=False):
    """
    Load a proxy:


    :param proxy: param can be:

        * Default -> use current proxy
        * string -> upload file specified as proxy
        * X509Chain -> use chain

    :returns:  S_OK/S_ERROR

      .. code-block:: python

          S_OK( { 'file' : <string with file location>,
                  'chain' : X509Chain object,
                  'tempFile' : <True if file is temporal>
                } )
          S_ERROR

    """
    tempFile = False
    # Set env
    if isinstance(proxy, X509Chain):
        tempFile = True
        retVal = writeChainToTemporaryFile(proxy)
        if not retVal["OK"]:
            return retVal
        proxyLoc = retVal["Value"]
    else:
        if not proxy:
            proxyLoc = getProxyLocation()
            if not proxyLoc:
                return S_ERROR(DErrno.EPROXYFIND)
        if isinstance(proxy, str):
            proxyLoc = proxy
        else:
            raise NotImplementedError(f"Unknown proxy type ({type(proxy)})")
        # Load proxy
        proxy = X509Chain()
        retVal = proxy.loadProxyFromFile(proxyLoc)
        if not retVal["OK"]:
            return S_ERROR(DErrno.EPROXYREAD, f"ProxyLocation: {proxyLoc}")
    return S_OK({"file": proxyLoc, "chain": proxy, "tempFile": tempFile})
