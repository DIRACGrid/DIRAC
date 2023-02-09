""" Container for TaskManager plug-ins, to handle the destination of the tasks
"""
from DIRAC import gLogger

from DIRAC.Core.Utilities.List import fromChar
from DIRAC.Core.Utilities.SiteSEMapping import getSitesForSE
from DIRAC.DataManagementSystem.Utilities.DMSHelpers import DMSHelpers
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getSites
from DIRAC.TransformationSystem.Client.PluginBase import PluginBase


class TaskManagerPlugin(PluginBase):
    """A TaskManagerPlugin object should be instantiated by every TaskManager object.

    self.params here could be
    {'Status': 'Created', 'TargetSE': 'Unknown', 'TransformationID': 1086L, 'RunNumber': 0L,
    'Site': 'DIRAC.Test.ch', 'TaskID': 21L, 'InputData': '', 'JobType': 'MCSimulation'}
    which corresponds to paramsDict in TaskManager (which is in fact a tasks dict)
    """

    def _BySE(self):
        """Matches using TargetSE. This is the standard plugin."""

        destSites = set()

        try:
            seList = ["Unknown"]
            if self.params["TargetSE"]:
                if isinstance(self.params["TargetSE"], str):
                    seList = fromChar(self.params["TargetSE"])
                elif isinstance(self.params["TargetSE"], list):
                    seList = self.params["TargetSE"]
        except KeyError:
            pass

        if not seList or seList == ["Unknown"]:
            return destSites

        for se in seList:
            res = getSitesForSE(se)
            if not res["OK"]:
                gLogger.warn("Could not get Sites associated to SE", res["Message"])
            else:
                thisSESites = res["Value"]
                if thisSESites:
                    # We make an OR of the possible sites
                    destSites.update(thisSESites)

        gLogger.debug(f"Destinations: {','.join(destSites)}")
        return destSites

    def _ByJobType(self):
        """By default, all sites are allowed to do every job.
        The actual rules are freely specified in the Operation JobTypeMapping section.
        The content of the section may look like this:

        User
        {
          Exclude = PAK
          Exclude += Ferrara
          Exclude += Bologna
          Exclude += Paris
          Exclude += CERN
          Exclude += IN2P3
          Allow
          {
            Paris = IN2P3
            CERN = CERN
            IN2P3 = IN2P3
          }
        }
        DataReconstruction
        {
          Exclude = PAK
          Exclude += Ferrara
          Exclude += CERN
          Exclude += IN2P3
          Allow
          {
            Ferrara = CERN
            CERN = CERN
            IN2P3 = IN2P3
            IN2P3 += CERN
          }
        }
        Merge
        {
          Exclude = ALL
          Allow
          {
            CERN = CERN
            IN2P3 = IN2P3
          }
        }

        The sites in the exclusion list will be removed.
        The allow section says where each site may help another site

        """
        # 1. get sites list
        res = getSites()
        if not res["OK"]:
            gLogger.error("Could not get the list of sites", res["Message"])
            return res
        destSites = set(res["Value"])

        # 2. get JobTypeMapping "Exclude" value (and add autoAddedSites)
        gLogger.debug("Getting JobTypeMapping 'Exclude' value (and add autoAddedSites)")
        jobType = self.params["JobType"]
        if not jobType:
            raise RuntimeError("No jobType specified")
        excludedSites = set(self.opsH.getValue(f"JobTypeMapping/{jobType}/Exclude", []))
        gLogger.debug(f"Explicitly excluded sites for {jobType} task: {','.join(excludedSites)}")
        autoAddedSites = self.opsH.getValue("JobTypeMapping/AutoAddedSites", [])
        if "WithStorage" in autoAddedSites:
            # Add all sites with storage, such that jobs can run wherever data is
            autoAddedSites.remove("WithStorage")
            autoAddedSites += DMSHelpers().getTiers(withStorage=True)

        # 3. removing sites in Exclude
        if not excludedSites:
            pass
        elif "ALL" in excludedSites:
            destSites = set()
        else:
            destSites -= excludedSites

        # 4. get JobTypeMapping "Allow" section
        res = self.opsH.getOptionsDict(f"JobTypeMapping/{jobType}/Allow")
        if not res["OK"]:
            gLogger.debug(res["Message"])
            allowed = {}
        else:
            allowed = {site: set(fromChar(fromSites)) for site, fromSites in res["Value"].items()}

        autoAddedSites = set(self.opsH.getValue(f"JobTypeMapping/{jobType}/AutoAddedSites", autoAddedSites))
        gLogger.debug(f"Auto-added sites for {jobType} task: {','.join(autoAddedSites)}")
        # 5. add autoAddedSites, if requested
        for autoAddedSite in autoAddedSites:
            allowed.setdefault(autoAddedSite, set()).add(autoAddedSite)
        gLogger.debug(f"Allowed sites for {jobType} task: {','.join(allowed)}")

        # 6. Allowing sites that should be allowed
        taskSiteDestination = self._BySE()

        for destSite, fromSites in allowed.items():
            for fromSite in fromSites:
                if not taskSiteDestination or fromSite in taskSiteDestination:
                    destSites.add(destSite)

        gLogger.debug(
            "Computed list of destination sites for %s task with TargetSE %s: %s"
            % (jobType, self.params["TargetSE"], ",".join(destSites))
        )
        return destSites
