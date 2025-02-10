"""
  Utilities for managing DIRAC configuration:
"""
from copy import deepcopy
import socket

from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Helpers.Path import cfgPath
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOs, getVOOption
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getDIRACSiteName
from DIRAC.ConfigurationSystem.Client.PathFinder import getDatabaseSection
from DIRAC.Core.Utilities.Glue2 import getGlue2CEInfo


def getGridVOs():
    """Get all the VOMS VO names served by this DIRAC service

    :return: S_OK(list)/S_ERROR()
    """
    voNames = []
    result = getVOs()
    if not result["OK"]:
        return result
    else:
        vos = result["Value"]
        for vo in vos:
            vomsVO = getVOOption(vo, "VOMSName")
            if vomsVO:
                voNames.append(vomsVO)
    return S_OK(voNames)


def getGridCEs(vo, bdiiInfo=None, ceBlackList=None, hostURL=None):
    """Get all the CEs available for a given VO and having queues in Production state

    :param str vo: VO name
    :param dict bddiInfo: information from BDII
    :param list ceBlackList: CEs from black list
    :param str hostURL: host URL

    :return: Dictionary with keys: OK, Value, BdiiInfo, UnknownCEs
    """
    knownCEs = set()
    cesInInformation = set()
    if ceBlackList is not None:
        knownCEs = knownCEs.union(set(ceBlackList))

    ceBdiiDict = bdiiInfo
    if bdiiInfo is None:
        result = getGlue2CEInfo(vo, host=hostURL)
        if not result["OK"]:
            return result
        ceBdiiDict = result["Value"]

    siteDict = {}
    for site in ceBdiiDict:
        siteCEs = set(ceBdiiDict[site]["CEs"].keys())
        cesInInformation.update(siteCEs)
        newCEs = siteCEs - knownCEs
        if not newCEs:
            continue

        ceFullDict = {}
        for ce in newCEs:
            ceDict = {}
            ceInfo = ceBdiiDict[site]["CEs"][ce]
            ceType = "Unknown"
            ceDict["Queues"] = []
            for queue in ceInfo["Queues"]:
                queueStatus = ceInfo["Queues"][queue].get("GlueCEStateStatus", "UnknownStatus")
                if "production" in queueStatus.lower():
                    ceType = ceInfo["Queues"][queue].get("GlueCEImplementationName", "")
                    ceDict["Queues"].append(queue)
            if not ceDict["Queues"]:
                continue

            ceDict["CEType"] = ceType
            ceDict["GOCSite"] = site
            ceDict["CEID"] = ce
            systemName = ceInfo.get("GlueHostOperatingSystemName", "Unknown")
            systemVersion = ceInfo.get("GlueHostOperatingSystemVersion", "Unknown")
            systemRelease = ceInfo.get("GlueHostOperatingSystemRelease", "Unknown")
            ceDict["System"] = (systemName, systemVersion, systemRelease)

            ceFullDict[ce] = ceDict

        siteDict[site] = ceFullDict

    result = S_OK(siteDict)
    result["BdiiInfo"] = ceBdiiDict

    unknownCEs = knownCEs - cesInInformation
    result["UnknownCEs"] = unknownCEs
    return result


def getSiteUpdates(vo, bdiiInfo=None, log=None, onecore=False):
    """Get all the necessary updates for the already defined sites and CEs

    :param str vo: VO name
    :param dict bdiiInfo: information from DBII
    :param object log: logger
    :param bool onecore: whether to add single core copies of multicore queues, see the documentation about :ref:`CE`
       and the :mod:`~DIRAC.ConfigurationSystem.Agent.Bdii2CSAgent` configuration for details

    :result: S_OK(set)/S_ERROR()
    """

    def addToChangeSet(entry, changeSet):
        """Inner function to update changeSet with entry (a tuple)

        :param tuple entry: entry to add to changeSet
        :param set changeSet: set collecting stuff to change
        """
        _section, _option, value, new_value = entry
        if new_value and new_value != value:
            changeSet.add(entry)

    def dropTag(tags, tagToDrop):
        """Remove tag from a comma-separated string of tags.

        :param str tags: the string of current tags
        :param str tagToDrop: the tag to potentially remove
        :return: string of comma separated tags
        """
        return ",".join(sorted(set(tags.split(",")).difference({tagToDrop}))).strip(",")

    def addTag(tags, tagToAdd):
        """Add tag to a comma-separated string of tags.

        :param str tags: the string of current tags
        :param str tagToAdd: the tag to potentially add
        :return: string of comma separated tags
        """
        return ",".join(sorted(set(tags.split(",")).union({tagToAdd}))).strip(",")

    if log is None:
        log = gLogger

    ceBdiiDict = bdiiInfo
    if bdiiInfo is None:
        result = getGlue2CEInfo(vo)
        if not result["OK"]:
            return result
        ceBdiiDict = result["Value"]

    if onecore:
        # If enabled this creates a copy of the queue with multiple processors and sets NumberOfProcessors to 1 for ARC
        # and HTCondorCE entries

        def makeNewQueueName(queueName, ceType):
            """Create a new queueName for single core queues."""
            if ceType == "HTCondorCE":
                return queueName + "1core"
            # we should have only ARC left, we add 1core to the middle part
            queueNameSplit = queueName.split("-", 2)
            queueNameSplit[1] = queueNameSplit[1] + "1core"
            return "-".join(queueNameSplit)

        for siteName, ceDict in ceBdiiDict.items():
            for _ceName, ceInfo in ceDict["CEs"].items():
                newQueues = dict()
                for queueName, queueDict in ceInfo["Queues"].items():
                    if (
                        queueDict["GlueCEImplementationName"] not in ("ARC", "HTCondorCE")
                        or int(queueDict.get("NumberOfProcessors", 1)) == 1
                    ):
                        continue
                    newQueueName = makeNewQueueName(queueName, queueDict["GlueCEImplementationName"])
                    newQueueDict = deepcopy(queueDict)
                    newQueueDict["NumberOfProcessors"] = 1
                    newQueues[newQueueName] = newQueueDict

                ceInfo["Queues"].update(newQueues)

    defaultLocalCEType = gConfig.getValue("/Resources/Computing/DefaultLocalCEType", "")

    changeSet = set()
    for site in ceBdiiDict:
        result = getDIRACSiteName(site)
        if not result["OK"]:
            continue
        siteNames = result["Value"]
        for siteName in siteNames:
            siteSection = cfgPath("/Resources", "Sites", siteName.split(".")[0], siteName)
            result = gConfig.getOptionsDict(siteSection)
            if not result["OK"]:
                continue
            siteDict = result["Value"]
            # Current CS values
            coor = siteDict.get("Coordinates", "Unknown")
            mail = siteDict.get("Mail", "Unknown").replace(" ", "")
            description = siteDict.get("Description", "Unknown")
            description = description.replace(" ,", ",")

            longitude = ceBdiiDict[site].get("GlueSiteLongitude", "").strip()
            latitude = ceBdiiDict[site].get("GlueSiteLatitude", "").strip()

            # Current BDII value
            newcoor = ""
            if longitude and latitude:
                newcoor = f"{longitude}:{latitude}"
            newmail = ceBdiiDict[site].get("GlueSiteSysAdminContact", "").replace("mailto:", "").strip()
            newdescription = ceBdiiDict[site].get("GlueSiteDescription", "").strip()
            newdescription = ", ".join([line.strip() for line in newdescription.split(",")])

            # Adding site data to the changes list
            addToChangeSet((siteSection, "Coordinates", coor, newcoor), changeSet)
            addToChangeSet((siteSection, "Mail", mail, newmail), changeSet)
            addToChangeSet((siteSection, "Description", description, newdescription), changeSet)

            ces = gConfig.getSections(cfgPath(siteSection, "CEs"))
            for ce in ces.get("Value", []):
                ceSection = cfgPath(siteSection, "CEs", ce)
                ceDict = {}
                result = gConfig.getOptionsDict(ceSection)
                if result["OK"]:
                    ceDict = result["Value"]
                else:
                    if ceBdiiDict[site]["CEs"].get(ce, None):
                        log.notice("Adding new CE", f"{ce} to site {siteName}/{site}")
                ceInfo = ceBdiiDict[site]["CEs"].get(ce, None)
                if ceInfo is None:
                    ceType = ceDict.get("CEType", "")
                    continue

                # Current CS CE info
                arch = ceDict.get("architecture", "Unknown")
                OS = ceDict.get("OS", "Unknown")
                si00 = ceDict.get("SI00", "Unknown")
                ceType = ceDict.get("CEType", "Unknown")
                ram = ceDict.get("MaxRAM", "Unknown")

                # Current BDII CE info
                newarch = ceBdiiDict[site]["CEs"][ce].get("GlueHostArchitecturePlatformType", "").strip()
                systemName = ceInfo.get("GlueHostOperatingSystemName", "").strip()
                systemVersion = ceInfo.get("GlueHostOperatingSystemVersion", "").strip()
                systemRelease = ceInfo.get("GlueHostOperatingSystemRelease", "").strip()
                newOS = ""
                if systemName and systemVersion and systemRelease:
                    newOS = "_".join((systemName, systemVersion, systemRelease))
                newsi00 = ceInfo.get("GlueHostBenchmarkSI00", "").strip()
                newCEType = "Unknown"
                for queue in ceInfo["Queues"]:
                    queueDict = ceInfo["Queues"][queue]
                    newCEType = queueDict.get("GlueCEImplementationName", "").strip()
                    if newCEType:
                        break
                if newCEType == "ARC-CE":
                    newCEType = "AREX"

                newRAM = ceInfo.get("GlueHostMainMemoryRAMSize", "").strip()
                # Protect from unreasonable values
                if newRAM and int(newRAM) > 150000:
                    newRAM = ""

                # Adding CE data to the change list
                addToChangeSet((ceSection, "architecture", arch, newarch), changeSet)
                addToChangeSet((ceSection, "OS", OS, newOS), changeSet)
                addToChangeSet((ceSection, "SI00", si00, newsi00), changeSet)

                addToChangeSet((ceSection, "CEType", ceType, newCEType), changeSet)

                addToChangeSet((ceSection, "MaxRAM", ram, newRAM), changeSet)

                for queue, queueInfo in ceInfo["Queues"].items():
                    queueStatus = queueInfo["GlueCEStateStatus"]
                    queueSection = cfgPath(ceSection, "Queues", queue)
                    queueDict = {}
                    result = gConfig.getOptionsDict(queueSection)
                    if result["OK"]:
                        queueDict = result["Value"]
                    else:
                        if queueStatus.lower() == "production":
                            log.notice("Adding new queue", f"{queue} to CE {ce}")
                        else:
                            continue

                    # Current CS queue info
                    maxCPUTime = queueDict.get("maxCPUTime", "Unknown")
                    si00 = queueDict.get("SI00", "Unknown")
                    maxTotalJobs = queueDict.get("MaxTotalJobs", "Unknown")

                    # Current BDII queue info
                    newMaxCPUTime = queueInfo.get("GlueCEPolicyMaxCPUTime", "")
                    if newMaxCPUTime == "4" * len(newMaxCPUTime) or newMaxCPUTime == "9" * len(newMaxCPUTime):
                        newMaxCPUTime = ""
                    wallTime = queueInfo.get("GlueCEPolicyMaxWallClockTime", "")
                    if wallTime == "4" * len(wallTime) or wallTime == "9" * len(wallTime):
                        wallTime = ""
                    if wallTime and int(wallTime) > 0:
                        if not newMaxCPUTime:
                            newMaxCPUTime = str(int(0.8 * int(wallTime)))
                        else:
                            if int(wallTime) <= int(newMaxCPUTime):
                                newMaxCPUTime = str(int(0.8 * int(wallTime)))
                    newSI00 = ""
                    caps = queueInfo.get("GlueCECapability", [])
                    if isinstance(caps, str):
                        caps = [caps]
                    for cap in caps:
                        if "CPUScalingReferenceSI00" in cap:
                            newSI00 = cap.split("=")[-1]

                    # tags, processors, localCEType
                    tag = queueDict.get("Tag", "")
                    reqTag = queueDict.get("RequiredTag", "")
                    # LocalCEType can be empty (equivalent to "InProcess")
                    # or "Pool", "Singularity", but also "Pool/Singularity"
                    localCEType = queueDict.get("LocalCEType", defaultLocalCEType)
                    try:
                        localCEType_inner = localCEType.split("/")[1]
                    except IndexError:
                        localCEType_inner = ""

                    numberOfProcessors = int(queueDict.get("NumberOfProcessors", 0))
                    newNOP = int(queueInfo.get("NumberOfProcessors", 1))

                    # Adding queue info to the CS
                    addToChangeSet((queueSection, "maxCPUTime", maxCPUTime, newMaxCPUTime), changeSet)
                    addToChangeSet((queueSection, "SI00", si00, newSI00), changeSet)

                    # add RequiredTag if onecore is enabled, do this here for previously created MultiCore queues
                    if newNOP > 1 and onecore:
                        addToChangeSet(
                            (queueSection, "RequiredTag", reqTag, addTag(reqTag, "MultiProcessor")), changeSet
                        )

                    if newNOP != numberOfProcessors:
                        addToChangeSet((queueSection, "NumberOfProcessors", numberOfProcessors, newNOP), changeSet)
                        if newNOP > 1:
                            # if larger than one, add MultiProcessor to site tags, and LocalCEType=Pool
                            addToChangeSet((queueSection, "Tag", tag, addTag(tag, "MultiProcessor")), changeSet)

                            if localCEType_inner:
                                newLocalCEType = "Pool/" + localCEType_inner
                            else:
                                newLocalCEType = "Pool"
                            addToChangeSet((queueSection, "LocalCEType", localCEType, newLocalCEType), changeSet)
                        else:
                            # if not larger than one, drop MultiProcessor Tag.
                            # Here we do not change the LocalCEType as Pool CE would still be perfectly valid.
                            changeSet.add((queueSection, "Tag", tag, dropTag(tag, "MultiProcessor")))
                            if onecore:
                                changeSet.add((queueSection, "RequiredTag", reqTag, dropTag(reqTag, "MultiProcessor")))

                    if maxTotalJobs == "Unknown":
                        newTotalJobs = min(1000, int(int(queueInfo.get("GlueCEInfoTotalCPUs", 0)) / 2))
                        newWaitingJobs = max(2, int(newTotalJobs * 0.1))
                        newTotalJobs = str(newTotalJobs)
                        newWaitingJobs = str(newWaitingJobs)
                        addToChangeSet((queueSection, "MaxTotalJobs", "", newTotalJobs), changeSet)
                        addToChangeSet((queueSection, "MaxWaitingJobs", "", newWaitingJobs), changeSet)

                    # Updating eligible VO list
                    VOs = set()
                    if queueDict.get("VO", ""):
                        VOs = {q.strip() for q in queueDict.get("VO", "").split(",") if q}
                    if vo not in VOs:
                        VOs.add(vo)
                        VOs = list(VOs)
                        newVOs = ",".join(VOs)
                        addToChangeSet((queueSection, "VO", "", newVOs), changeSet)

    return S_OK(changeSet)


def getDBParameters(fullname):
    """Retrieve Database parameters from CS

    :param str fullname: should be of the form <System>/<DBname>

    :return: S_OK(dict)/S_ERROR() - dictionary with the keys: 'host', 'port', 'user', 'password',
                                    'db' and 'queueSize'
    """

    cs_path = getDatabaseSection(fullname)
    parameters = {}

    # Check mandatory parameters first: Password, User, Host and DBName
    result = gConfig.getOption(cs_path + "/Password")
    if not result["OK"]:
        # No individual password found, try at the common place
        result = gConfig.getOption("/Systems/Databases/Password")
        if not result["OK"]:
            return S_ERROR("Failed to get the configuration parameter: Password")
    dbPass = result["Value"]
    parameters["Password"] = dbPass

    result = gConfig.getOption(cs_path + "/User")
    if not result["OK"]:
        # No individual user name found, try at the common place
        result = gConfig.getOption("/Systems/Databases/User")
        if not result["OK"]:
            return S_ERROR("Failed to get the configuration parameter: User")
    dbUser = result["Value"]
    parameters["User"] = dbUser

    result = gConfig.getOption(cs_path + "/Host")
    if not result["OK"]:
        # No host name found, try at the common place
        result = gConfig.getOption("/Systems/Databases/Host")
        if not result["OK"]:
            return S_ERROR("Failed to get the configuration parameter: Host")
    dbHost = result["Value"]
    # Check if the host is the local one and then set it to 'localhost' to use
    # a socket connection
    if dbHost != "localhost":
        localHostName = socket.getfqdn()
        if localHostName == dbHost:
            dbHost = "localhost"
    parameters["Host"] = dbHost

    result = gConfig.getOption(cs_path + "/DBName")
    if not result["OK"]:
        return S_ERROR("Failed to get the configuration parameter: DBName")
    dbName = result["Value"]
    parameters["DBName"] = dbName

    # Check optional parameters: Port
    # Mysql standard
    dbPort = 3306
    result = gConfig.getOption(cs_path + "/Port")
    if not result["OK"]:
        # No individual port number found, try at the common place
        result = gConfig.getOption("/Systems/Databases/Port")
        if result["OK"]:
            dbPort = int(result["Value"])
    else:
        dbPort = int(result["Value"])
    parameters["Port"] = dbPort

    return S_OK(parameters)


def getElasticDBParameters(fullname):
    """Retrieve Database parameters from CS

    :param str fullname: should be of the form <System>/<DBname>

    :return: S_OK(dict)/S_ERROR()
    """

    def _getCACerts(cs_path):
        result = gConfig.getOption(cs_path + "/ca_certs")
        if not result["OK"]:
            # No CA certificate found, try at the common place
            result = gConfig.getOption("/Systems/NoSQLDatabases/ca_certs")
            if not result["OK"]:
                return None
            else:
                ca_certs = result["Value"]
        else:
            ca_certs = result["Value"]
        return ca_certs

    cs_path = getDatabaseSection(fullname)
    parameters = {}

    # Check if connection is through certificates and get certificate parameters
    # OpenSearch use certs
    result = gConfig.getOption(cs_path + "/CRT")
    if not result["OK"]:
        # No CRT option found, try at the common place
        result = gConfig.getOption("/Systems/NoSQLDatabases/CRT")
        if not result["OK"]:
            gLogger.debug("Failed to get the configuration parameter: CRT. Using False")
            certs = False
        else:
            certs = result["Value"].lower() in ("true", "yes", "y", "1")
    else:
        certs = result["Value"].lower() in ("true", "yes", "y", "1")
    parameters["CRT"] = certs

    # If connection is through certificates get the mandatory parameters: ca_certs, client_key, client_cert
    if parameters["CRT"]:
        parameters["Password"] = None
        parameters["User"] = None

        ca_certs = _getCACerts(cs_path)
        if ca_certs is None:
            return S_ERROR("Failed to get the configuration parameter: ca_certs.")
        parameters["ca_certs"] = ca_certs

        # OpenSearch client_key
        result = gConfig.getOption(cs_path + "/client_key")
        if not result["OK"]:
            # No client private key found, try at the common place
            result = gConfig.getOption("/Systems/NoSQLDatabases/client_key")
            if not result["OK"]:
                return S_ERROR("Failed to get the configuration parameter: client_key.")
            else:
                client_key = result["Value"]
        else:
            client_key = result["Value"]
        parameters["client_key"] = client_key

        # OpenSearch client_cert
        result = gConfig.getOption(cs_path + "/client_cert")
        if not result["OK"]:
            # No cient certificate found, try at the common place
            result = gConfig.getOption("/Systems/NoSQLDatabases/client_cert")
            if not result["OK"]:
                return S_ERROR("Failed to get the configuration parameter: client_cert.")
            else:
                client_cert = result["Value"]
        else:
            client_cert = result["Value"]
        parameters["client_cert"] = client_cert
    # If connection is not through certificates get the mandatory parameters: Password, User
    else:
        result = gConfig.getOption(cs_path + "/Password")
        if not result["OK"]:
            # No individual password found, try at the common place
            result = gConfig.getOption("/Systems/NoSQLDatabases/Password")
            if not result["OK"]:
                return S_ERROR("Failed to get the configuration parameter: Password.")
        dbPass = result["Value"]
        parameters["Password"] = dbPass

        result = gConfig.getOption(cs_path + "/User")
        if not result["OK"]:
            # No individual user name found, try at the common place
            result = gConfig.getOption("/Systems/NoSQLDatabases/User")
            if not result["OK"]:
                return S_ERROR("Failed to get the configuration parameter: User.")
        dbUser = result["Value"]
        parameters["User"] = dbUser

        # ca_certs is not mandatory
        ca_certs = _getCACerts(cs_path)
        if ca_certs:
            parameters["ca_certs"] = ca_certs

    # Check optional parameters: Host, Port, SSL
    result = gConfig.getOption(cs_path + "/Host")
    if not result["OK"]:
        # No host name found, try at the common place
        result = gConfig.getOption("/Systems/NoSQLDatabases/Host")
        if not result["OK"]:
            gLogger.warn("Failed to get the configuration parameter: Host. Using localhost")
            dbHost = "localhost"
        else:
            dbHost = result["Value"]
    else:
        dbHost = result["Value"]
    # Check if the host is the local one and then set it to 'localhost' to use
    # a socket connection
    if dbHost != "localhost":
        localHostName = socket.getfqdn()
        if localHostName == dbHost:
            dbHost = "localhost"
    parameters["Host"] = dbHost

    # OpenSearch standard port
    result = gConfig.getOption(cs_path + "/Port")
    if not result["OK"]:
        # No individual port number found, try at the common place
        result = gConfig.getOption("/Systems/NoSQLDatabases/Port")
        if not result["OK"]:
            gLogger.debug("No configuration parameter set for Port, assuming URL points to right location")
            dbPort = None
        else:
            dbPort = int(result["Value"])
    else:
        dbPort = int(result["Value"])
    parameters["Port"] = dbPort

    result = gConfig.getOption(cs_path + "/SSL")
    if not result["OK"]:
        # No SSL option found, try at the common place
        result = gConfig.getOption("/Systems/NoSQLDatabases/SSL")
        if not result["OK"]:
            gLogger.debug("Failed to get the configuration parameter: SSL. Assuming SSL is needed")
            ssl = True
        else:
            ssl = result["Value"].lower() in ("true", "yes", "y", "1")
    else:
        ssl = result["Value"].lower() in ("true", "yes", "y", "1")
    parameters["SSL"] = ssl

    return S_OK(parameters)


def getDIRACGOCDictionary():
    """
    Create a dictionary containing DIRAC site names and GOCDB site names
    using a configuration provided by CS.

    :return:  A dictionary of DIRAC site names (key) and GOCDB site names (value).
    """

    log = gLogger.getSubLogger("getDIRACGOCDictionary")
    log.debug("Begin function ...")

    result = gConfig.getConfigurationTree("/Resources/Sites", "Name")
    if not result["OK"]:
        log.error(f"getConfigurationTree() failed with message: {result['Message']}")
        return S_ERROR("Configuration is corrupted")
    siteNamesTree = result["Value"]

    dictionary = dict()
    PATHELEMENTS = 6  # site names have 6 elements in the path, i.e.:
    #    /Resource/Sites/<GRID NAME>/<DIRAC SITE NAME>/Name
    # [0]/[1]     /[2]  /[3]        /[4]              /[5]

    for path, gocdbSiteName in siteNamesTree.items():  # can be an iterator
        elements = path.split("/")
        if len(elements) != PATHELEMENTS:
            continue

        diracSiteName = elements[PATHELEMENTS - 2]
        dictionary[diracSiteName] = gocdbSiteName

    log.debug("End function.")
    return S_OK(dictionary)


def getAuthAPI():
    """Get Auth REST API url

    :return: str
    """
    return gConfig.getValue(f"/Systems/Framework/URLs/AuthAPI")


def getAuthorizationServerMetadata(issuer=None, ignoreErrors=False):
    """Get authorization server metadata

    :param str issuer: issuer
    :param bool ignoreErrors: igrnore configuration errors

    :return: S_OK(dict)/S_ERROR()
    """
    data = {}
    try:
        result = gConfig.getOptionsDictRecursively("/DIRAC/Security/Authorization")
        if not result["OK"]:
            return S_OK({"issuer": issuer}) if issuer else result
        data = result["Value"]
    except Exception as e:
        if ignoreErrors:
            gLogger.warn(repr(e))
        else:
            raise e

    # Search DIRAC Authorization Server issuer
    data["issuer"] = data.get("issuer", issuer)
    if not data["issuer"]:
        try:
            data["issuer"] = getAuthAPI()
        except Exception as e:
            return S_ERROR(f"No issuer found in DIRAC authorization server: {repr(e)}")

    return S_OK(data) if data["issuer"] else S_ERROR("Cannot find DIRAC Authorization Server issuer.")


def isDownloadProxyAllowed():
    """Get allowProxyDownload flag

    :return: S_OK(bool)/S_ERROR()
    """
    cs_path = f"/Systems/Framework/APIs/Auth"
    return gConfig.getValue(cs_path + "/allowProxyDownload", True)
