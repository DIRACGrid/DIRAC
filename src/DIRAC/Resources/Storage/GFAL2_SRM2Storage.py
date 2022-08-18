""" :mod: GFAL2_SRM2Storage

    =================

    .. module: python

    :synopsis: SRM2 module based on the GFAL2_StorageBase class.
"""
# pylint: disable=invalid-name


import errno
import json

import gfal2  # pylint: disable=import-error

# from DIRAC
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Resources.Storage.GFAL2_StorageBase import GFAL2_StorageBase
from DIRAC.Resources.Storage.Utilities import checkArgumentFormat


class GFAL2_SRM2Storage(GFAL2_StorageBase):
    """SRM2 SE class that inherits from GFAL2StorageBase"""

    _INPUT_PROTOCOLS = [
        "file",
        "gsiftp",
        "https",
        "root",
        "srm",
    ]
    _OUTPUT_PROTOCOLS = ["gsiftp", "https", "root", "srm"]

    def __init__(self, storageName, parameters):
        """ """
        super().__init__(storageName, parameters)
        self.log = gLogger.getSubLogger(self.__class__.__name__)
        self.log.debug("GFAL2_SRM2Storage.__init__: Initializing object")
        self.pluginName = "GFAL2_SRM2"

        # This attribute is used to know the file status (OFFLINE,NEARLINE,ONLINE)
        self._defaultExtendedAttributes = ["user.status"]

        # ##
        #    Setting the default SRM parameters here. For methods where this
        #    is not the default there is a method defined in this class, setting
        #    the proper values and then calling the base class method.
        # ##

        self.gfal2requestLifetime = gConfig.getValue("/Resources/StorageElements/RequestLifeTime", 100)

        self.protocolsList = self.protocolParameters["OutputProtocols"]
        self.log.debug(f"GFAL2_SRM2Storage: protocolsList = {self.protocolsList}")

        self.__setSRMOptionsToDefault()

    def __setSRMOptionsToDefault(self):
        """Resetting the SRM options back to default"""
        self.ctx.set_opt_integer("SRM PLUGIN", "OPERATION_TIMEOUT", self.gfal2Timeout)
        if self.spaceToken:
            self.ctx.set_opt_string("SRM PLUGIN", "SPACETOKENDESC", self.spaceToken)
        self.ctx.set_opt_integer("SRM PLUGIN", "REQUEST_LIFETIME", self.gfal2requestLifetime)
        # Setting the TURL protocol to gsiftp because with other protocols we have authorisation problems
        self.ctx.set_opt_string_list("SRM PLUGIN", "TURL_PROTOCOLS", self.protocolsList)
        self.ctx.set_opt_string_list("SRM PLUGIN", "TURL_3RD_PARTY_PROTOCOLS", self.protocolsList)

    def _updateMetadataDict(self, metadataDict, attributeDict):
        """Updating the metadata dictionary with srm specific attributes

        :param self: self reference
        :param dict: metadataDict we want add the SRM specific attributes to
        :param dict: attributeDict contains 'user.status' which we then fill in the metadataDict

        """
        # 'user.status' is the extended attribute we are interested in
        user_status = attributeDict.get("user.status", "")
        metadataDict["Cached"] = int("ONLINE" in user_status)
        metadataDict["Migrated"] = int("NEARLINE" in user_status)
        metadataDict["Lost"] = int(user_status == "LOST")
        metadataDict["Unavailable"] = int(user_status == "UNAVAILABLE")
        metadataDict["Accessible"] = (
            not metadataDict["Lost"] and metadataDict["Cached"] and not metadataDict["Unavailable"]
        )

    def getTransportURL(self, path, protocols=False):
        """obtain the tURLs for the supplied path and protocols

        :param self: self reference
        :param str path: path on storage
        :param mixed protocols: protocols to use
        :returns: Failed dict {path : error message}
                 Successful dict {path : transport url}
                 S_ERROR in case of argument problems
        """
        res = checkArgumentFormat(path)
        if not res["OK"]:
            return res
        urls = res["Value"]

        self.log.debug("GFAL2_SRM2Storage.getTransportURL: Attempting to retrieve tURL for %s paths" % len(urls))

        failed = {}
        successful = {}
        if not protocols:
            listProtocols = self.protocolsList
            if not listProtocols:
                return S_ERROR("GFAL2_SRM2Storage.getTransportURL: No local protocols defined and no defaults found.")
        elif isinstance(protocols, str):
            listProtocols = [protocols]
        elif isinstance(protocols, list):
            listProtocols = protocols
        else:
            return S_ERROR("getTransportURL: Must supply desired protocols to this plug-in.")

        # Compatibility because of castor returning a castor: url if you ask
        # for a root URL, and a root: url if you ask for a xroot url...
        if "root" in listProtocols and "xroot" not in listProtocols:
            listProtocols.insert(listProtocols.index("root"), "xroot")
        elif "xroot" in listProtocols and "root" not in listProtocols:
            listProtocols.insert(listProtocols.index("xroot") + 1, "root")

        if self.protocolParameters["Protocol"] in listProtocols:
            successful = {}
            failed = {}
            for url in urls:
                if self.isURL(url)["Value"]:
                    successful[url] = url
                else:
                    failed[url] = "getTransportURL: Failed to obtain turls."

            return S_OK({"Successful": successful, "Failed": failed})

        for url in urls:
            res = self.__getSingleTransportURL(url, listProtocols)
            self.log.debug("res = %s" % res)

            if not res["OK"]:
                failed[url] = res["Message"]
            else:
                successful[url] = res["Value"]

        return S_OK({"Failed": failed, "Successful": successful})

    def __getSingleTransportURL(self, path, protocols=False):
        """Get the tURL from path with getxattr from gfal2

        :param self: self reference
        :param str path: path on the storage
        :returns: S_OK( Transport_URL ) in case of success
                  S_ERROR( errStr ) in case of a failure
        """
        self.log.debug("GFAL2_SRM2Storage.__getSingleTransportURL: trying to retrieve tURL for %s" % path)
        if protocols:
            self.ctx.set_opt_string_list("SRM PLUGIN", "TURL_PROTOCOLS", protocols)

        res = self._getExtendedAttributes(path, attributes=["user.replicas"])
        self.__setSRMOptionsToDefault()

        if res["OK"]:
            return S_OK(res["Value"]["user.replicas"])

        errStr = "GFAL2_SRM2Storage.__getSingleTransportURL: Extended attribute tURL is not set."
        self.log.debug(errStr, res["Message"])
        return res

    def getOccupancy(self, *parms, **kws):
        """Gets the GFAL2_SRM2Storage occupancy info.

        TODO: needs gfal2.15 because of bugs:
        https://its.cern.ch/jira/browse/DMC-979
        https://its.cern.ch/jira/browse/DMC-977

        It queries the srm interface for a given space token.
        Out of the results, we keep totalsize, guaranteedsize, and unusedsize all in B.
        """

        if not self.spaceToken:
            self.log.info("getOccupancy: SpaceToken not defined for this SE. Falling back to the default getOccupancy.")
            return super().getOccupancy(*parms, **kws)

        # Gfal2 extended parameter name to query the space token occupancy
        spaceTokenAttr = "spacetoken.description?%s" % self.protocolParameters["SpaceToken"]
        # gfal2 can take any srm url as a base.
        spaceTokenEndpoint = self.getURLBase(withWSUrl=True)["Value"]
        try:
            occupancyStr = self.ctx.getxattr(spaceTokenEndpoint, spaceTokenAttr)
            try:
                occupancyDict = json.loads(occupancyStr)[0]
            except ValueError:
                # https://its.cern.ch/jira/browse/DMC-977
                # a closing bracket is missing, so we retry after adding it
                occupancyStr = occupancyStr[:-1] + "}]"
                occupancyDict = json.loads(occupancyStr)[0]

                # https://its.cern.ch/jira/browse/DMC-979
                # We set totalsize to guaranteed size
                # (it is anyway true for all the SEs I could test)
                occupancyDict["totalsize"] = occupancyDict.get("guaranteedsize", 0)

        except (gfal2.GError, ValueError) as e:
            errStr = "Something went wrong while checking for spacetoken occupancy."
            self.log.verbose(errStr, e.message)
            return S_ERROR(getattr(e, "code", errno.EINVAL), f"{errStr} {repr(e)}")

        sTokenDict = {}

        sTokenDict["Total"] = float(occupancyDict.get("totalsize", "0"))
        sTokenDict["Free"] = float(occupancyDict.get("unusedsize", "0"))
        sTokenDict["SpaceReservation"] = self.protocolParameters["SpaceToken"]

        return S_OK(sTokenDict)
