""" :mod: GFAL2_XROOTStorage

    =================

    .. module: python

    :synopsis: XROOT module based on the GFAL2_StorageBase class.
"""
import os

# from DIRAC
from DIRAC import gLogger
from DIRAC.Resources.Storage.GFAL2_StorageBase import GFAL2_StorageBase
from DIRAC.Core.Utilities.Pfn import pfnparse, pfnunparse
from DIRAC.Core.Security.Locations import getProxyLocation

sLog = gLogger.getSubLogger(__name__)


class GFAL2_XROOTStorage(GFAL2_StorageBase):
    """.. class:: GFAL2_XROOTStorage

    Xroot interface to StorageElement using gfal2
    """

    _INPUT_PROTOCOLS = ["file", "root", "xroot"]
    _OUTPUT_PROTOCOLS = ["root"]

    PROTOCOL_PARAMETERS = GFAL2_StorageBase.PROTOCOL_PARAMETERS + ["SvcClass"]
    DYNAMIC_OPTIONS = {"SvcClass": "svcClass"}

    def __init__(self, storageName, parameters):
        """c'tor

        :param self: self reference
        :param str storageName: SE name
        :param str protocol: protocol to use
        :param str rootdir: base path for vo files
        :param str host: SE host
        :param int port: port to use to communicate with :host:
        :param str spaceToken: space token
        :param str wspath: location of SRM on :host:
        """
        # # init base class
        super().__init__(storageName, parameters)
        self.srmSpecificParse = False

        self.log = sLog.getSubLogger(storageName)

        self.pluginName = "GFAL2_XROOT"

        # why is this here ?!
        self.protocolParameters["WSUrl"] = 0
        self.protocolParameters["SpaceToken"] = 0

        # We don't need extended attributes for metadata
        self._defaultExtendedAttributes = None

        # Because some storages are configured to use krb5 auth first
        # we end up in trouble for interactive sessions. This
        # environment variable enforces the use of certificates
        if "XrdSecPROTOCOL" not in os.environ:
            os.environ["XrdSecPROTOCOL"] = "gsi,unix"

        # needed for third party copy
        # emulates the gfal2 behaviour: https://its.cern.ch/jira/browse/DMC-1150
        if "XrdSecGSIDELEGPROXY" not in os.environ:
            os.environ["XrdSecGSIDELEGPROXY"] = "1"

    def __addDoubleSlash(self, res):
        """Utilities to add the double slash between the host(:port) and the path

        :param res: DIRAC return structure which contains an URL if S_OK
        :return: DIRAC structure with corrected URL
        """
        if not res["OK"]:
            return res
        url = res["Value"]
        res = pfnparse(url, srmSpecific=self.srmSpecificParse)
        if not res["OK"]:
            return res
        urlDict = res["Value"]
        urlDict["Path"] = "/" + urlDict["Path"]

        # Now, that's one heck of a disgusting hack
        # xrootd client is a bit faulty when managing
        # the connection cache, and ends up reusing an
        # existing connection for different users (security flaw...)
        # they have fixed it (to some extent starting from xrootd 4.10)
        # (https://github.com/xrootd/xrootd/issues/976)
        # BUT. They still can't consume properly the information when
        # the identity is passed in the url (root://url?gsiusrpxy=/tmp/myproxy)
        # So we apply a trick here which is to specify the proxy filename as a virtual user
        # This has no consequence (developer's own words), but to distinguish between users
        # Another ticket has been opened for that https://github.com/xrootd/xrootd/issues/992

        try:
            proxyLoc = getProxyLocation()
            if proxyLoc:
                # xroot does not support dots in the virtual user
                proxyLoc = os.path.basename(proxyLoc).replace(".", "")
                urlDict["Host"] = f"{proxyLoc}@{urlDict['Host']}"
        except Exception as e:
            self.log.warn("Exception trying to add virtual user in the url", repr(e))

        return pfnunparse(urlDict, srmSpecific=self.srmSpecificParse)

    def getURLBase(self, withWSUrl=False):
        """Overwrite to add the double slash"""
        return self.__addDoubleSlash(super().getURLBase(withWSUrl=withWSUrl))

    def constructURLFromLFN(self, lfn, withWSUrl=False):
        """Overwrite to add the double slash"""
        return self.__addDoubleSlash(super().constructURLFromLFN(lfn=lfn, withWSUrl=withWSUrl))

    def getCurrentURL(self, fileName):
        """Overwrite to add the double slash"""
        return self.__addDoubleSlash(super().getCurrentURL(fileName))
