"""FreeDiskSpaceCommand

Command to retrieve and cache the free/total disk space of a Storage Element.

The unit and decision thresholds are read from ``self.args``, which are populated
by the policy engine from ``POLICIESMETA`` defaults and any CS overrides:

* ``unit``               — space unit for the occupancy query (``TB``, ``GB`` or ``MB``)
* ``Banned_threshold``   — free-space value below which the SE is Banned
* ``Degraded_threshold`` — free-space value below which the SE is Degraded
* ``Banned_fraction``    — fraction of total space below which the SE is Banned
* ``Degraded_fraction``  — fraction of total space below which the SE is Degraded

Note: there are still many references to "space tokens" (e.g.
``ResourceManagementClient().selectSpaceTokenOccupancyCache(token=elementName)``).
This is for historical reasons; when you see "token" or "space token" here, read "StorageElement".

"""

import errno
import sys
from datetime import datetime, timedelta

from DIRAC import S_ERROR, S_OK
from DIRAC.AccountingSystem.Client.DataStoreClient import gDataStoreClient
from DIRAC.AccountingSystem.Client.Types.StorageOccupancy import StorageOccupancy
from DIRAC.Core.Utilities.File import convertSizeUnits
from DIRAC.DataManagementSystem.Utilities.DMSHelpers import DMSHelpers
from DIRAC.Resources.Storage.StorageElement import StorageElement
from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient
from DIRAC.ResourceStatusSystem.Command.Command import Command
from DIRAC.ResourceStatusSystem.Utilities import CSHelpers


class FreeDiskSpaceCommand(Command):
    """
    Command that queries the occupancy of a Storage Element and caches the result.

    Occupancy values are stored in the SpaceTokenOccupancyCache table (in MB) and
    recorded in the StorageOccupancy accounting. The unit used for the returned
    values is configurable (default: TB).
    """

    def __init__(self, args=None, clients=None):
        super().__init__(args, clients=clients)

        self.rmClient = ResourceManagementClient()

    def _prepareCommand(self):
        """
        Extract and validate command arguments from ``self.args``.

        Required key:

        * ``name`` (str) — Storage Element name.

        Optional keys (populated from ``POLICIESMETA`` defaults and CS overrides):

        * ``unit`` (str)                 — space unit: ``TB``, ``GB`` or ``MB``.
        * ``Banned_threshold`` (float)   — free space below which the SE is Banned.
        * ``Degraded_threshold`` (float) — free space below which the SE is Degraded.
        * ``Banned_fraction`` (float)    — fraction of total space below which the SE is Banned.
        * ``Degraded_fraction`` (float)  — fraction of total space below which the SE is Degraded.

        :returns: S_OK tuple ``(elementName, unit, banned_threshold, degraded_threshold,
            banned_fraction, degraded_fraction)`` or S_ERROR if ``name`` is missing.
        """

        if "name" not in self.args:
            return S_ERROR('"name" not found in self.args')
        elementName = self.args["name"]

        unit = self.args["unit"]
        banned_threshold = self.args["Banned_threshold"]
        degraded_threshold = self.args["Degraded_threshold"]
        banned_fraction = self.args["Banned_fraction"]
        degraded_fraction = self.args["Degraded_fraction"]

        return S_OK((elementName, unit, banned_threshold, degraded_threshold, banned_fraction, degraded_fraction))

    def doNew(self, masterParams=None):
        """
        Query the SE occupancy directly and cache the result.

        Fetches the free and total disk space from the Storage Element, stores the
        values in the SpaceTokenOccupancyCache table (in MB) and in the StorageOccupancy
        accounting, then returns them to the caller in the configured unit together with
        the decision thresholds.

        :param masterParams: when called from ``doMaster``, a ``(name, unit)`` tuple
            that overrides ``self.args``; otherwise ``None``.

        :returns: S_OK dict with keys ``Free``, ``Total``, ``Banned_threshold``,
            ``Degraded_threshold``, ``Banned_fraction``, ``Degraded_fraction`` (all in the configured unit), or S_ERROR.
        """

        if masterParams is not None:
            elementName, unit = masterParams
            banned_threshold = self.args["Banned_threshold"]
            degraded_threshold = self.args["Degraded_threshold"]
            banned_fraction = self.args["Banned_fraction"]
            degraded_fraction = self.args["Degraded_fraction"]
        else:
            params = self._prepareCommand()
            if not params["OK"]:
                return params
            (
                elementName,
                unit,
                banned_threshold,
                degraded_threshold,
                banned_fraction,
                degraded_fraction,
            ) = params["Value"]

        se = StorageElement(elementName)
        occupancyResult = se.getOccupancy(unit=unit)
        if not occupancyResult["OK"]:
            return occupancyResult
        occupancy = occupancyResult["Value"]
        free = occupancy["Free"]
        total = occupancy["Total"]

        results = {"Endpoint": "Deprecated", "Free": free, "Total": total, "ElementName": elementName}
        result = self._storeCommand(results)
        if not result["OK"]:
            return result

        return S_OK(
            {
                "Free": free,
                "Total": total,
                "Banned_threshold": banned_threshold,
                "Degraded_threshold": degraded_threshold,
                "Banned_fraction": banned_fraction,
                "Degraded_fraction": degraded_fraction,
            }
        )

    def _storeCommand(self, results):
        """
        Persist occupancy data to the cache table and accounting system.

        Writes to SpaceTokenOccupancyCache (values in MB) and registers
        Free/Total/Used records in the StorageOccupancy accounting type.

        :param dict results: occupancy data, e.g.::

            {
                'ElementName': 'CERN-HIST-EOS',
                'Endpoint':    'httpg://srm-eoslhcb-bis.cern.ch:8443/srm/v2/server',
                'Free':        3264963586.10073,   # MB
                'Total':       8000000000.0,        # MB
            }

        :returns: S_OK on success, S_ERROR otherwise.
        """

        # Stores in cache
        res = self.rmClient.addOrModifySpaceTokenOccupancyCache(
            endpoint=results["Endpoint"],
            lastCheckTime=datetime.utcnow(),
            free=results["Free"],
            total=results["Total"],
            token=results["ElementName"],
        )
        if not res["OK"]:
            self.log.error("Error calling addOrModifySpaceTokenOccupancyCache", res["Message"])
            return res

        # Now proceed with the accounting
        siteRes = DMSHelpers().getLocalSiteForSE(results["ElementName"])
        if not siteRes["OK"]:
            return siteRes

        accountingDict = {
            "StorageElement": results["ElementName"],
            "Endpoint": results["Endpoint"],
            "Site": siteRes["Value"] if siteRes["Value"] else "unassigned",
        }

        # There are sometimes small discrepencies which can lead to negative
        # used values.
        results["Used"] = max(0, results["Total"] - results["Free"])

        for sType in ["Total", "Free", "Used"]:
            spaceTokenAccounting = StorageOccupancy()
            spaceTokenAccounting.setNowAsStartAndEndTime()
            spaceTokenAccounting.setValuesFromDict(accountingDict)
            spaceTokenAccounting.setValueByKey("SpaceType", sType)
            spaceTokenAccounting.setValueByKey("Space", int(convertSizeUnits(results[sType], "MB", "B")))

            res = gDataStoreClient.addRegister(spaceTokenAccounting)
            if not res["OK"]:
                self.log.warn("Could not commit register", res["Message"])
                continue

        return gDataStoreClient.commit()

    def doCache(self):
        """
        Retrieve SE occupancy from the SpaceTokenOccupancyCache table.

        Values are stored in MB and converted on the fly to the configured unit
        before being returned. The decision thresholds are appended to the result
        so that ``FreeDiskSpacePolicy`` can evaluate them without re-reading the CS.

        :returns: S_OK dict with keys ``Free``, ``Total``, ``Banned_threshold``,
            ``Degraded_threshold``, ``Banned_fraction``, ``Degraded_fraction``
            (all in the configured unit), or S_ERROR if no cached record exists
            or the unit is invalid.
        """

        params = self._prepareCommand()
        if not params["OK"]:
            return params
        elementName, unit, banned_threshold, degraded_threshold, banned_fraction, degraded_fraction = params["Value"]

        result = self.rmClient.selectSpaceTokenOccupancyCache(token=elementName)

        if not result["OK"]:
            return result
        if not result["Value"]:
            return S_ERROR(errno.ENODATA, "No occupancy recorded")

        # results are normally in 'MB'
        free = result["Value"][0][3]
        total = result["Value"][0][4]

        free = convertSizeUnits(free, "MB", unit)
        total = convertSizeUnits(total, "MB", unit)

        if free == -sys.maxsize or total == -sys.maxsize:
            return S_ERROR("No valid unit specified")

        return S_OK(
            {
                "Free": free,
                "Total": total,
                "Banned_threshold": banned_threshold,
                "Degraded_threshold": degraded_threshold,
                "Banned_fraction": banned_fraction,
                "Degraded_fraction": degraded_fraction,
            }
        )

    def doMaster(self):
        """
        Refresh occupancy data for all Storage Elements known to the CS.

        Iterates over all SEs returned by DMSHelpers, calls ``doNew`` for each one
        (always using MB as the internal storage unit), then purges stale entries
        from the cache via ``_cleanCommand``.

        :returns: S_OK on success, S_ERROR if the cache cleanup fails.
        """

        for name in DMSHelpers().getStorageElements():
            try:
                diskSpace = self.doNew((name, "MB"))
                if not diskSpace["OK"]:
                    self.log.warn("Unable to calculate free/total disk space", f"name: {name}")
                    self.log.warn(diskSpace["Message"])
                    continue
            except Exception as excp:  # pylint: disable=broad-except
                self.log.error("Failed to get SE FreeDiskSpace information ==> SE skipped", name)
                self.log.exception("Operation finished with exception: ", lException=excp)

        # Clear the cache
        return self._cleanCommand()

    def _cleanCommand(self, toDelete=None):
        """
        Remove stale entries from the SpaceTokenOccupancyCache table.

        An entry is considered stale when its ``LastCheckTime`` is older than 6 hours
        and the corresponding SE/endpoint pair no longer exists in the CS.

        :param tuple toDelete: if provided, a single ``(endpoint, storage_element_name)``
            tuple to delete explicitly, e.g. ``('httpg://srm-lhcb.cern.ch:8443/srm/managerv2', 'CERN-RAW')``.
            If ``None`` (default), stale entries are detected automatically.

        :returns: S_OK always (individual deletion failures are logged as warnings).
        """
        if not toDelete:
            toDelete = []

            res = self.rmClient.selectSpaceTokenOccupancyCache(
                meta={"older": ["LastCheckTime", datetime.utcnow() - timedelta(hours=6)]}
            )
            if not res["OK"]:
                return res
            storedSEsSet = {(sse[0], sse[1]) for sse in res["Value"]}

            currentSEsSet = set()
            currentSEs = DMSHelpers().getStorageElements()
            for cse in currentSEs:
                res = CSHelpers.getStorageElementEndpoint(cse)
                if not res["OK"]:
                    self.log.warn("Could not get endpoint", res["Message"])
                    continue
                endpoint = res["Value"][0]

                currentSEsSet.add((endpoint, cse))
            toDelete = list(storedSEsSet - currentSEsSet)

        else:
            toDelete = [toDelete]

        for ep in toDelete:
            res = self.rmClient.deleteSpaceTokenOccupancyCache(ep[0], ep[1])
            if not res["OK"]:
                self.log.warn("Could not delete entry from SpaceTokenOccupancyCache", res["Message"])

        return S_OK()
