"""
  Defines the plugin to take storage space information given by BDII
"""
import os

from DIRAC import S_OK
from DIRAC.Core.Utilities.Grid import ldapsearchBDII


class BDIIOccupancy:
    """.. class:: BDIIOccupancy

    Occupancy plugin to return the space information given by BDII
    Assuming the protocol is SRM
    """

    def __init__(self, se):
        # flag to show initalization status of the plugin
        self.log = se.log.getSubLogger("BDIIOccupancy")
        # BDII host to query
        self.bdii = "cclcgtopbdii01.in2p3.fr:2170"
        if "LCG_GFAL_INFOSYS" in os.environ:
            self.bdii = os.environ["LCG_GFAL_INFOSYS"]
        self.vo = se.vo
        # assume given SE speaks SRM
        ret = se.getStorageParameters(protocol="srm")
        if not ret["OK"]:
            raise RuntimeError(ret["Message"])
        if "Host" not in ret["Value"]:
            raise RuntimeError("No Host is found from StorageParameters")
        self.host = ret["Value"]["Host"]

    def getOccupancy(self, **kwargs):
        """Returns the space information given by BDII
        Total and Free space are taken from GlueSATotalOnlineSize and GlueSAFreeOnlineSize, respectively.

        :returns: S_OK with dict (keys: Total, Free)
        """
        sTokenDict = {"Total": 0, "Free": 0}
        BDIIAttr = ["GlueSATotalOnlineSize", "GlueSAFreeOnlineSize"]

        filt = f"(&(GlueSAAccessControlBaseRule=VO:{self.vo})(GlueChunkKey=GlueSEUniqueID={self.host}))"
        ret = ldapsearchBDII(filt, BDIIAttr, host=self.bdii)
        if not ret["OK"]:
            return ret
        for value in ret["Value"]:
            if "attr" in value:
                attr = value["attr"]
                sTokenDict["Total"] = float(attr.get(BDIIAttr[0], 0)) * 1024 * 1024 * 1024
                sTokenDict["Free"] = float(attr.get(BDIIAttr[1], 0)) * 1024 * 1024 * 1024
        return S_OK(sTokenDict)
