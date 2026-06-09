"""DowntimeCommand

Command to fetch and cache GOCDB downtime information for RSS-managed Sites and Resources.
Downtimes found are stored in the DowntimeCache table via ResourceManagementClient.
Stale or deleted GOCDB downtimes are also removed from the cache.

The look-ahead window is controlled by the ``hours`` argument read from ``self.args``,
populated by the policy engine from ``POLICIESMETA`` defaults and any CS overrides:

* ``hours = 0`` — only ongoing downtimes are considered.
* ``hours > 0`` — downtimes starting within the next ``hours`` hours are also included.

"""

import re
from datetime import datetime, timedelta
from operator import itemgetter
from urllib.error import URLError

from DIRAC import S_ERROR, S_OK, gConfig
from DIRAC.ConfigurationSystem.Client.Helpers.Path import cfgPath
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import (
    getCESiteMapping,
    getFTS3Servers,
    getGOCFTSName,
    getGOCSiteName,
    getGOCSites,
)
from DIRAC.Core.LCG.GOCDBClient import GOCDBClient
from DIRAC.Core.Utilities.SiteSEMapping import getSEHosts, getStorageElementsHosts
from DIRAC.Resources.Storage.StorageElement import StorageElement
from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient
from DIRAC.ResourceStatusSystem.Command.Command import Command

# conversion from DIRAC resource type to GOCDB service type
diracToGOC_conversion = {
    # Computing elements
    "HTCondorCE": "org.opensciencegrid.htcondorce",
    "AREX": "ARC-CE",
    # FTS
    "FTS3": "FTS",
    "FTS": "FTS",
    # Storage elements
    "disk_srm": "srm",
    "tape_srm": "srm.nearline",
    "disk_root": "xrootd",
    "tape_root": "wlcg.xrootd.tape",
    "disk_https": "webdav",
    "tape_https": "wlcg.webdav.tape",
}


class DowntimeCommand(Command):
    """
    Command that queries GOCDB for downtime information and caches the results.

    Supports Sites, Storage Elements, FTS servers, and Computing Elements.
    DIRAC resource types are mapped to GOCDB service types via ``diracToGOC_conversion``.
    """

    def __init__(self, args=None, clients=None):
        super().__init__(args, clients)

        self.gClient = self.apis.get("GOCDBClient", GOCDBClient())
        self.rmClient = self.apis.get("ResourceManagementClient", ResourceManagementClient())

    def _storeCommand(self, result):
        """
        Persist a list of downtime records to the DowntimeCache table.

        :param list result: list of downtime dicts, each containing ``DowntimeID``,
            ``Element``, ``Name``, ``StartDate``, ``EndDate``, ``Severity``,
            ``Description``, ``Link``, and ``gOCDBServiceType``.

        :returns: S_OK / S_ERROR from the last ``addOrModifyDowntimeCache`` call.
        """

        for dt in result:
            resQuery = self.rmClient.addOrModifyDowntimeCache(
                downtimeID=dt["DowntimeID"],
                element=dt["Element"],
                name=dt["Name"],
                startDate=dt["StartDate"],
                endDate=dt["EndDate"],
                severity=dt["Severity"],
                description=dt["Description"],
                link=dt["Link"],
                gOCDBServiceType=dt["gOCDBServiceType"],
            )
        return resQuery

    def _cleanCommand(self, element, elementNames):
        """
        Remove expired or deleted downtime entries from the DowntimeCache.

        A cached entry is removed if its ``EndDate`` is in the past or if its
        GOCDB link no longer appears in the list of current GOCDB downtimes.

        :param str element: ``'Site'`` or ``'Resource'``.
        :param list elementNames: names of the elements whose cache entries should be checked.

        :returns: S_OK with a list of deletion results, or S_ERROR on DB / GOCDB failure.
        """

        resQuery = []

        for elementName in elementNames:
            # get the list of all DTs stored in the cache
            result = self.rmClient.selectDowntimeCache(element=element, name=elementName)

            if not result["OK"]:
                return result

            uniformResult = [dict(zip(result["Columns"], res)) for res in result["Value"]]

            currentDate = datetime.utcnow()

            if not uniformResult:
                continue

            # get the list of all ongoing DTs from GocDB
            gDTLinkList = self.gClient.getCurrentDTLinkList()
            if not gDTLinkList["OK"]:
                return gDTLinkList

            for dt in uniformResult:
                # if DT expired or DT not in the list of current DTs, then we remove it from the cache
                if dt["EndDate"] < currentDate or dt["Link"] not in gDTLinkList["Value"]:
                    result = self.rmClient.deleteDowntimeCache(downtimeID=dt["DowntimeID"])
                    resQuery.append(result)

        return S_OK(resQuery)

    def _prepareCommand(self):
        """
        Extract and validate command arguments from ``self.args``, resolving DIRAC
        names to their GOCDB equivalents where necessary.

        Required keys:

        * ``name`` (str)        — DIRAC element name.
        * ``element`` (str)     — ``'Site'`` or ``'Resource'``.
        * ``elementType`` (str) — resource type (e.g. ``StorageElement``, ``ComputingElement``, ``FTS3``).

        Optional key:

        * ``hours`` (int) — look-ahead window in hours (populated from ``POLICIESMETA``).

        Name resolution:

        * **Site** — converted to the GOCDB site name via ``getGOCSiteName``.
        * **StorageElement** — resolved to one or more SE hosts; GOCDB service type
          derived from SE type and access protocol via ``diracToGOC_conversion``.
        * **FTS / FTS3** — resolved to the GOCDB FTS name via ``getGOCFTSName``.
        * **ComputingElement** — GOCDB service type derived from CE type via ``diracToGOC_conversion``.

        :returns: S_OK tuple ``(element, elementName, hours, gOCDBServiceType)`` or S_ERROR.
        """

        if not self.args.get("name"):
            return S_ERROR('"name" not found in self.args')
        elementName = self.args["name"]

        if not self.args.get("element"):
            return S_ERROR('"element" not found in self.args')
        element = self.args["element"]

        if not self.args.get("elementType"):
            return S_ERROR('"elementType" not found in self.args')
        elementType = self.args["elementType"]

        if element not in ["Site", "Resource"]:
            return S_ERROR("element is neither Site nor Resource")

        hours = self.args.get("hours")

        gOCDBServiceType = None

        # Transform DIRAC site names into GOCDB topics
        if element == "Site":
            gocSite = getGOCSiteName(elementName)
            if not gocSite["OK"]:  # The site is most probably is not a grid site - not an issue, of course
                pass  # so, elementName remains unchanged
            else:
                elementName = gocSite["Value"]

        # The DIRAC SE names mean nothing on the grid, but their hosts and service types do mean.
        elif elementType == "StorageElement":
            # Get the SE object and its protocols
            try:
                se = StorageElement(elementName)
                se_protocols = list(se.localAccessProtocolList)
                se_protocols.extend(x for x in se.localWriteProtocolList if x not in se_protocols)
            except AttributeError:
                self.log.error("Failure instantiating StorageElement object", elementName)
                return S_ERROR("Failure instantiating StorageElement")

            # Determine the SE type and update gOCDBServiceType accordingly
            se_type = se.options.get("SEType", "")
            diskOrTape = ""
            if re.search(r"D[1-9]", se_type):
                diskOrTape = "disk"
            elif re.search(r"T[1-9]", se_type):
                diskOrTape = "tape"
            # iterate on the protocols and get the first one
            for protocol in se_protocols:
                dirac_protocol = f"{diskOrTape}_{protocol}"
                if dirac_protocol in diracToGOC_conversion:
                    gOCDBServiceType = diracToGOC_conversion[dirac_protocol]
                    break

            # Get the SE hosts and return an error if none are found
            res = getSEHosts(elementName)
            if not res["OK"]:
                return res
            seHosts = res["Value"]

            if not seHosts:
                return S_ERROR(f"No seHost(s) for {elementName}")
            elementName = seHosts  # in this case it will return a list, because there might be more than one host only

        elif elementType in ["FTS", "FTS3"]:
            try:
                gOCDBServiceType = diracToGOC_conversion[elementType]
            except KeyError:  # not a GOC type (? how can this happen ?)
                gOCDBServiceType = None
            gocSite = getGOCFTSName(elementName)
            if not gocSite["OK"]:
                self.log.warn("FTS not in Resources/FTSEndpoints/FTS3 ?", elementName)
            else:
                elementName = gocSite["Value"]

        elif elementType == "ComputingElement":
            res = getCESiteMapping(elementName)
            if not res["OK"]:
                return res
            siteName = res["Value"][elementName]
            ceType = gConfig.getValue(
                cfgPath("Resources", "Sites", siteName.split(".")[0], siteName, "CEs", elementName, "CEType")
            )
            try:
                gOCDBServiceType = diracToGOC_conversion[ceType]
            except KeyError:  # not a GOC type (e.g. SSH CE)
                gOCDBServiceType = None

        return S_OK((element, elementName, hours, gOCDBServiceType))

    def doNew(self, masterParams=None):
        """
        Fetch current downtime information from GOCDB and store it in the cache.

        Queries GOCDB for ongoing (and optionally upcoming) downtimes for the given
        element(s). The GOCDB server is queried twice on ``URLError`` to handle
        transient failures. Found downtimes are stored via ``_storeCommand``; the
        cache is cleaned of stale entries via ``_cleanCommand``.

        :param masterParams: when called from ``doMaster``, a ``(element, elementNames)``
            tuple (e.g. ``('Site', ['CERN', 'IN2P3-CC'])``); the look-ahead window is
            taken from ``self.args.get('hours', 0)``. Pass ``None`` to use ``self.args``
            directly (normal per-element policy evaluation path).

        :returns: S_OK on success (value is ``None`` if no downtimes were found), or S_ERROR.
        """

        if masterParams is not None:
            element, elementNames = masterParams
            hours = self.args.get("hours", 0)
            elementName = None
            gOCDBServiceType = None

        else:
            params = self._prepareCommand()
            if not params["OK"]:
                return params
            element, elementName, hours, gOCDBServiceType = params["Value"]
            if not isinstance(elementName, list):
                elementNames = [elementName]
            else:
                elementNames = elementName

        # WARNING: checking all the DT that are ongoing or starting in given <hours> from now
        try:
            results = self.gClient.getStatus(element, name=elementNames, startingInHours=hours)
        except URLError:
            try:
                # Let's give it a second chance..
                results = self.gClient.getStatus(element, name=elementNames, startingInHours=hours)
            except URLError as e:
                return S_ERROR(e)

        if not results["OK"]:
            return results
        results = results["Value"]

        if results is None:  # no downtimes found
            return S_OK(None)

        # cleaning the Cache
        if elementNames:
            if not (res := self._cleanCommand(element, elementNames))["OK"]:
                return res

        uniformResult = []

        # Humanize the results into a dictionary, not the most optimal, but readable
        for downtime, downDic in results.items():  # can be an iterator
            dt = {}

            dt["Name"] = downDic.get("URL", downDic.get("HOSTNAME", downDic.get("SITENAME")))
            if not dt["Name"]:
                return S_ERROR("URL, SITENAME and HOSTNAME are missing from downtime dictionary")

            dt["gOCDBServiceType"] = downDic.get("SERVICE_TYPE")

            if dt["gOCDBServiceType"] and gOCDBServiceType:
                if gOCDBServiceType.lower() != downDic["SERVICE_TYPE"].lower():
                    self.log.warn(
                        "SERVICE_TYPE mismatch",
                        "between GOCDB (%s) and CS (%s) for %s"
                        % (downDic["SERVICE_TYPE"], gOCDBServiceType, dt["Name"]),
                    )

            dt["DowntimeID"] = downtime
            dt["Element"] = element
            dt["StartDate"] = downDic["FORMATED_START_DATE"]
            dt["EndDate"] = downDic["FORMATED_END_DATE"]
            dt["Severity"] = downDic["SEVERITY"]
            dt["Description"] = downDic["DESCRIPTION"].replace("'", "")
            dt["Link"] = downDic["GOCDB_PORTAL_URL"]

            uniformResult.append(dt)

        if not (res := self._storeCommand(uniformResult))["OK"]:
            return res

        return S_OK()

    def doCache(self):
        """
        Retrieve the most relevant downtime for this element from the DowntimeCache.

        When ``hours`` is set, the target date is shifted into the future and the
        earliest matching downtime is returned (useful for advance warning of scheduled
        outages). When ``hours`` is ``None``, ongoing downtimes are evaluated and the
        highest-severity, longest-lasting one is returned.

        Priority when multiple downtimes overlap:

        * OUTAGE takes precedence over WARNING.
        * Among equal severity: the one ending latest wins (``hours=None`` path) or
          the one starting earliest wins (``hours>0`` path).

        :returns: S_OK with a downtime dict (keys: ``DowntimeID``, ``Element``, ``Name``,
            ``StartDate``, ``EndDate``, ``Severity``, ``Description``, ``Link``,
            ``gOCDBServiceType``) if a relevant downtime exists, S_OK with ``None``
            if no downtime applies, or S_ERROR on DB failure.
        """

        params = self._prepareCommand()
        if not params["OK"]:
            return params
        element, elementName, hours, gOCDBServiceType = params["Value"]

        result = self.rmClient.selectDowntimeCache(element=element, name=elementName, gOCDBServiceType=gOCDBServiceType)
        if not result["OK"]:
            return result
        if not result["Value"]:
            return S_OK()

        uniformResult = [dict(zip(result["Columns"], res)) for res in result["Value"]]

        # 'targetDate' can be either now or in some 'hours' from now
        targetDate = datetime.utcnow()

        # dtOverlapping is a buffer to assure only one dt is returned
        # when there are overlapping outage/warning dt for same element
        # on top of the buffer we put the most recent outages
        # while at the bottom the most recent warnings,
        # assumption: uniformResult list is already ordered by resource/site name, severity, startdate
        dtOverlapping = []

        if hours is not None:
            # IN THE FUTURE
            targetDate = targetDate + timedelta(hours=hours)
            # sorting by 'StartDate' b/c if we look for DTs in the future
            # then we are interested in the earliest DTs
            uniformResult.sort(key=itemgetter("Name", "Severity", "StartDate"))

            for dt in uniformResult:
                if (dt["StartDate"] < targetDate) and (dt["EndDate"] > targetDate):
                    # the list is already ordered in a way that outages come first over warnings
                    # and the earliest outages are on top of other outages and warnings
                    # while the earliest warnings are on top of the other warnings
                    # so what ever comes first in the list is also what we are looking for
                    dtOverlapping = [dt]
                    break
        else:
            # IN THE PRESENT
            # sorting by 'EndDate' b/c if we look for DTs in the present
            # then we are interested in those DTs that last longer
            uniformResult.sort(key=itemgetter("Name", "Severity", "EndDate"))

            for dt in uniformResult:
                if (dt["StartDate"] < targetDate) and (dt["EndDate"] > targetDate):
                    # if outage, we put it on top of the overlapping buffer
                    # i.e. the latest ending outage is on top
                    if dt["Severity"].upper() == "OUTAGE":
                        dtOverlapping = [dt] + dtOverlapping
                    # if warning, we put it at the bottom of the overlapping buffer
                    # i.e. the latest ending warning is at the bottom
                    elif dt["Severity"].upper() == "WARNING":
                        dtOverlapping.append(dt)

        if not dtOverlapping:
            return S_OK()

        dtTop = dtOverlapping[0]
        if dtTop["Severity"].upper() == "OUTAGE":
            return S_OK(dtTop)
        else:
            return S_OK(dtOverlapping[-1])

    def doMaster(self):
        """
        Refresh downtime data for all known Sites and Resources from GOCDB.

        Collects:

        * All GOCDB site names (from ``getGOCSites``).
        * All SE hosts (from ``getStorageElementsHosts``).
        * All FTS3 server hosts (from ``getFTS3Servers``).
        * All Computing Element names (from ``getCESiteMapping``).

        Calls ``doNew`` separately for Sites and Resources. Failures are recorded in
        ``self.metrics['failed']`` but do not abort the run.

        :returns: S_OK with ``self.metrics`` dict (containing a ``'failed'`` list).
        """

        gocSites = getGOCSites()
        if not gocSites["OK"]:
            return gocSites
        gocSites = gocSites["Value"]

        sesHosts = getStorageElementsHosts()
        if not sesHosts["OK"]:
            return sesHosts
        sesHosts = sesHosts["Value"]

        resources = sesHosts if sesHosts else []

        ftsServer = getFTS3Servers(hostOnly=True)
        if ftsServer["OK"] and ftsServer["Value"]:
            resources.extend(ftsServer["Value"])

        # TODO: file catalogs need also to use their hosts

        # fc = CSHelpers.getFileCatalogs()
        # if fc[ 'OK' ]:
        #  resources = resources + fc[ 'Value' ]

        res = getCESiteMapping()
        if res["OK"] and res["Value"]:
            resources.extend(list(res["Value"]))

        self.log.verbose("Processing Sites", ", ".join(gocSites if gocSites else ["NONE"]))

        siteRes = self.doNew(("Site", gocSites))
        if not siteRes["OK"]:
            self.metrics["failed"].append(siteRes["Message"])

        self.log.verbose("Processing Resources", ", ".join(resources if resources else ["NONE"]))

        resourceRes = self.doNew(("Resource", resources))
        if not resourceRes["OK"]:
            self.metrics["failed"].append(resourceRes["Message"])

        return S_OK(self.metrics)
