"""
  Defines the plugin to take storage space information given by WLCG Accounting Json
  https://twiki.cern.ch/twiki/bin/view/LCG/AccountingTaskForce#Storage_Space_Accounting
  https://twiki.cern.ch/twiki/pub/LCG/AccountingTaskForce/storage_service_v4.txt
  https://docs.google.com/document/d/1yzCvKpxsbcQC5K9MyvXc-vBF1HGPBk4vhjw3MEXoXf8

  When this is used, the OccupancyLFN has to be the full path on the storage, and not just the LFN
"""
import json
import os
import tempfile
import shutil
import errno
import gfal2  # pylint: disable=import-error

from DIRAC import S_OK, S_ERROR


class WLCGAccountingJson:
    """.. class:: WLCGAccountingJson

    Occupancy plugin to return the space information given by WLCG Accouting Json
    """

    def __init__(self, se):
        self.se = se
        self.log = se.log.getSubLogger("WLCGAccountingJson")
        self.name = self.se.name

    def _downloadJsonFile(self, occupancyLFN, filePath):
        """Download the json file at the location

        :param occupancyLFN: lfn for the file
        :param filePath: destination path for the file

        """
        for storage in self.se.storages.values():
            try:
                ctx = gfal2.creat_context()
                params = ctx.transfer_parameters()
                params.overwrite = True
                res = storage.updateURL(occupancyLFN)
                if not res["OK"]:
                    continue
                occupancyURL = res["Value"]
                ctx.filecopy(params, occupancyURL, "file://" + filePath)
                return
            except gfal2.GError as e:
                detailMsg = "Failed to copy file %s to destination url %s: [%d] %s" % (
                    occupancyURL,
                    filePath,
                    e.code,
                    e.message,
                )
                self.log.debug("Exception while copying", detailMsg)
                continue

    def getOccupancy(self, **kwargs):
        """Returns the space information given by WLCG Accouting Json

        :returns: S_OK with dict (keys: SpaceReservation, Total, Free)
        """
        occupancyLFN = kwargs["occupancyLFN"]

        if not occupancyLFN:
            return S_ERROR("Failed to get occupancyLFN")

        tmpDirName = tempfile.mkdtemp()
        filePath = os.path.join(tmpDirName, os.path.basename(occupancyLFN))

        self._downloadJsonFile(occupancyLFN, filePath)

        if not os.path.isfile(filePath):
            return S_ERROR("No WLCGAccountingJson file of %s is downloaded." % (self.name))

        with open(filePath) as path:
            occupancyDict = json.load(path)

        # delete temp dir
        shutil.rmtree(tmpDirName)

        try:
            storageShares = occupancyDict["storageservice"]["storageshares"]
        except KeyError as e:
            return S_ERROR(errno.ENOMSG, f"Issue finding storage shares. {repr(e)} in {occupancyLFN} at {self.name}.")

        spaceReservation = self.se.options.get("SpaceReservation")

        # get storageshares in WLCGAccountingJson file
        storageSharesSR = None
        if spaceReservation:
            for storageshare in storageShares:
                if storageshare.get("name") == spaceReservation:
                    storageSharesSR = storageshare
                    break
        else:
            self.log.debug(
                "Could not find SpaceReservation in CS, and get storageShares and spaceReservation from WLCGAccoutingJson."
            )
            shareLen = []
            for storage in self.se.storages.values():
                basePath = storage.getParameters()["Path"]
                for share in storageShares:
                    shareLen.append((share, len(os.path.commonprefix([share["path"][0], basePath]))))
            storageSharesSR = max(shareLen, key=lambda x: x[1])[0]
            spaceReservation = storageSharesSR.get("name")

        sTokenDict = {}
        sTokenDict["SpaceReservation"] = spaceReservation
        try:
            sTokenDict["Total"] = storageSharesSR["totalsize"]
            sTokenDict["Free"] = storageSharesSR.get("freesize", sTokenDict["Total"] - storageSharesSR["usedsize"])
        except KeyError as e:
            return S_ERROR(
                errno.ENOMSG,
                f"Issue finding Total or Free space left. {repr(e)} in {spaceReservation} storageshares.",
            )

        return S_OK(sTokenDict)
