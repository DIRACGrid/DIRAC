"""
  Pure XROOT servers do not generate the standard WLCG accounting json file
  This plugin uses direct xroot query to retrieve the information. The ``OccupancyLFN``
  should point to a directory over which we have permissions and where quota are potentially
  defined (accounting and quotas are not first class citizens in xrootd world)
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import errno
from XRootD import client

from XRootD.client.flags import QueryCode
from DIRAC import S_OK, S_ERROR


class XRootOccupancy(object):
    """
    Occupancy plugin to return the space information given by a pure Xroot storage
    """

    def __init__(self, se):
        self.se = se
        self.name = self.se.name

    def getOccupancy(self, **kwargs):
        """Returns the space information given by Xroot query

        :returns: S_OK with dict (keys: SpaceReservation, Total, Free)
        """

        occupancyLFN = kwargs["occupancyLFN"]
        if not occupancyLFN:
            return S_ERROR("Failed to get occupancyLFN")

        # Get the xroot plugin configuration
        rootOptions = [po for po in self.se.protocolOptions if po["Protocol"] == "root"][0]

        rootClient = client.FileSystem(rootOptions["Host"])
        status, response = rootClient.query(QueryCode.SPACE, occupancyLFN)

        if not status.ok:
            return S_ERROR(status.message)

        responseDict = dict(p.split("=") for p in response.decode().split("&"))

        spaceReservation = self.se.options.get("SpaceReservation")

        sTokenDict = {}
        sTokenDict["SpaceReservation"] = spaceReservation
        try:
            sTokenDict["Total"] = int(responseDict["oss.space"])
            sTokenDict["Free"] = int(responseDict["oss.free"])
        except KeyError as e:
            return S_ERROR(
                errno.ENOMSG,
                "Issue finding Total or Free space left. %s in %s storageshares." % (repr(e), spaceReservation),
            )

        return S_OK(sTokenDict)
