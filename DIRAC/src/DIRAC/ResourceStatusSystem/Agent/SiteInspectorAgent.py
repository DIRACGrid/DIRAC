""" SiteInspectorAgent

  This agent inspect Sites, and evaluates policies that apply.

The following options can be set for the SiteInspectorAgent.

.. literalinclude:: ../ConfigTemplate.cfg
  :start-after: ##BEGIN SiteInspectorAgent
  :end-before: ##END
  :dedent: 2
  :caption: SiteInspectorAgent options
"""
import datetime
import concurrent.futures

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.ResourceStatusSystem.PolicySystem.PEP import PEP

AGENT_NAME = "ResourceStatus/SiteInspectorAgent"


class SiteInspectorAgent(AgentModule):
    """SiteInspectorAgent

    The SiteInspectorAgent agent is an agent that is used to get the all the site names
    and trigger PEP to evaluate their status.
    """

    # Max number of worker threads by default
    __maxNumberOfThreads = 15

    # Inspection freqs, defaults, the lower, the higher priority to be checked.
    # Error state usually means there is a glitch somewhere, so it has the highest
    # priority.
    __checkingFreqs = {"Active": 20, "Degraded": 20, "Probing": 20, "Banned": 15, "Unknown": 10, "Error": 5}

    def __init__(self, *args, **kwargs):
        AgentModule.__init__(self, *args, **kwargs)

        self.rsClient = None
        self.clients = {}

    def initialize(self):
        """Standard initialize."""

        res = ObjectLoader().loadObject("DIRAC.ResourceStatusSystem.Client.ResourceManagementClient")
        if not res["OK"]:
            self.log.error(f"Failed to load ResourceManagementClient class: {res['Message']}")
            return res
        rmClass = res["Value"]

        res = ObjectLoader().loadObject("DIRAC.ResourceStatusSystem.Client.ResourceStatusClient")
        if not res["OK"]:
            self.log.error(f"Failed to load ResourceStatusClient class: {res['Message']}")
            return res
        rsClass = res["Value"]

        self.rsClient = rsClass()
        self.clients["ResourceStatusClient"] = rsClass()
        self.clients["ResourceManagementClient"] = rmClass()

        maxNumberOfThreads = self.am_getOption("maxNumberOfThreads", 15)
        self.log.info("Multithreaded with %d threads" % maxNumberOfThreads)
        self.threadPoolExecutor = concurrent.futures.ThreadPoolExecutor(max_workers=maxNumberOfThreads)

        return S_OK()

    def execute(self):
        """
        It gets the sites from the Database which are eligible to be re-checked.
        """

        utcnow = datetime.datetime.utcnow().replace(microsecond=0)
        future_to_element = {}

        # get the current status
        res = self.rsClient.selectStatusElement("Site", "Status")
        if not res["OK"]:
            return res

        # filter elements
        for site in res["Value"]:
            # Maybe an overkill, but this way I have NEVER again to worry about order
            # of elements returned by mySQL on tuples
            siteDict = dict(zip(res["Columns"], site))

            # This if-clause skips all the elements that should not be checked yet
            timeToNextCheck = self.__checkingFreqs[siteDict["Status"]]
            if utcnow <= siteDict["LastCheckTime"] + datetime.timedelta(minutes=timeToNextCheck):
                continue

            # We skip the elements with token different than "rs_svc"
            if siteDict["TokenOwner"] != "rs_svc":
                self.log.verbose(f"Skipping {siteDict['Name']} with token {siteDict['TokenOwner']}")
                continue

            # if we are here, we process the current element
            self.log.verbose(f"\"{siteDict['Name']}\" # {siteDict['Status']} # {siteDict['LastCheckTime']}")

            lowerElementDict = {"element": "Site"}
            for key, value in siteDict.items():
                if len(key) >= 2:  # VO !
                    lowerElementDict[key[0].lower() + key[1:]] = value
            # We process lowerElementDict
            future = self.threadPoolExecutor.submit(self._execute, lowerElementDict)
            future_to_element[future] = siteDict["Name"]

        for future in concurrent.futures.as_completed(future_to_element):
            transID = future_to_element[future]
            try:
                future.result()
            except Exception as exc:
                self.log.exception(f"{transID} generated an exception: {exc}")
            else:
                self.log.info("Processed", transID)

        return S_OK()

    def _execute(self, site):
        """
        Method run by each of the thread that is in the ThreadPool.
        It evaluates the policies for such site and enforces the necessary actions.
        """

        pep = PEP(clients=self.clients)

        self.log.verbose(
            "%s ( VO=%s / status=%s / statusType=%s ) being processed"
            % (site["name"], site["vO"], site["status"], site["statusType"])
        )

        try:
            res = pep.enforce(site)
        except Exception:
            self.log.exception("Exception during enforcement")
            res = S_ERROR("Exception during enforcement")
        if not res["OK"]:
            self.log.error("Failed policy enforcement", res["Message"])
            return res

        resEnforce = res["Value"]

        oldStatus = resEnforce["decisionParams"]["status"]
        statusType = resEnforce["decisionParams"]["statusType"]
        newStatus = resEnforce["policyCombinedResult"]["Status"]
        reason = resEnforce["policyCombinedResult"]["Reason"]

        if oldStatus != newStatus:
            self.log.info(f"{site['name']} ({statusType}) is now {newStatus} ( {reason} ), before {oldStatus}")

    def finalize(self):
        """graceful finalization"""

        self.log.info("Wait for threads to get empty before terminating the agent")
        self.threadPoolExecutor.shutdown()
        self.log.info("Threads are empty, terminating the agent...")
        return S_OK()
