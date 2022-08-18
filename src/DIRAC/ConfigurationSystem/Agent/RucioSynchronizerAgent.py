""" :mod: RucioSynchronizer

  Agent that synchronizes Rucio and Dirac

  The following options can be set for the RucioSynchronizerAgent.

.. literalinclude:: ../ConfigTemplate.cfg
  :start-after: ##BEGIN RucioSynchronizerAgent
  :end-before: ##END
  :dedent: 2
  :caption: RucioSynchronizerAgent options
"""

# # imports
from itertools import permutations
from traceback import format_exc

from rucio.client import Client
from rucio.common.exception import RSEProtocolNotSupported, Duplicate, RSEAttributeNotFound

from DIRAC import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.Core.Security import Locations
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import (
    getUserOption,
    getAllUsers,
    getHosts,
    getVOs,
    getHostOption,
    getAllGroups,
    getDNsInGroup,
    getUsersInVO,
    getGroupsForVO,
)
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.DataManagementSystem.Utilities.DMSHelpers import DMSHelpers, resolveSEGroup
from DIRAC.Resources.Storage.StorageElement import StorageElement
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getCatalogPath


def getStorageElements(vo):
    """
    Get configuration of storage elements

    :param vo: VO name that an SE supports
    :return: S_OK/S_ERROR, Value dictionary with key SE and value protocol list
    """
    log = gLogger.getLocalSubLogger("RucioSynchronizer/%s" % vo)
    seProtocols = {}
    dms = DMSHelpers(vo=vo)
    for seName in dms.getStorageElements():
        se = StorageElement(seName)
        if not se.valid:
            log.warn("Storage element is not valid.", seName)
            continue
        if vo not in se.options.get("VO", []):
            log.debug("SE is valid, but it doesn't support the VO. Skipped.", f"[SE: {seName}, VO: {vo}]")
            continue
        log.debug(" Processing a valid SE for VO: ", f"[SE:{seName}, VO:{vo}]")
        log.debug("Available SE options ", se.options)
        seProtocols[seName] = []
        all_protocols = []
        read_protocols = {}
        protocols = se.options.get("AccessProtocols")
        log.debug("Global AccessProtocols:", f"[VO: {vo}, protocols: {protocols}]")
        if not protocols:
            protocols = dms.getAccessProtocols()
            if not protocols:
                log.warn(" No global or SE specific access protocols defined for SE ", seName)
                continue
        log.debug("AccessProtocols:", f"[VO: {vo}, protocols:{protocols}]")
        idx = 1
        for prot in protocols:
            read_protocols[prot] = idx
            idx += 1
            if prot not in all_protocols:
                all_protocols.append(prot)
        write_protocols = {}
        protocols = se.options.get("WriteProtocols")
        if not protocols:
            if not protocols:
                protocols = dms.getWriteProtocols()
                if not protocols:
                    log.warn(" No global or SE specific write protocols defined for SE ", seName)
                    continue
        idx = 1
        for prot in protocols:
            write_protocols[prot] = idx
            idx += 1
            if prot not in all_protocols:
                all_protocols.append(prot)

        mapping = {"Protocol": "scheme", "Host": "hostname", "Port": "port", "Path": "prefix"}
        for protocol in all_protocols:
            space_token = None
            params = {
                "hostname": None,
                "scheme": None,
                "port": None,
                "prefix": None,
                "impl": "rucio.rse.protocols.gfal.Default",
                "domains": {
                    "lan": {"read": 0, "write": 0, "delete": 0},
                    "wan": {"read": 0, "write": 0, "delete": 0, "third_party_copy": 0},
                },
            }
            res = se.getStorageParameters(protocol=protocol)
            if res["OK"]:
                values = res["Value"]
                for key in ["Protocol", "Host", "Access", "Path", "Port", "WSUrl", "SpaceToken", "WSUrl", "PluginName"]:
                    value = values.get(key)
                    if key in mapping:
                        params[mapping[key]] = value
                    else:
                        if key == "SpaceToken":
                            space_token = value
                        if params["scheme"] == "srm" and key == "WSUrl":
                            params["extended_attributes"] = {
                                "web_service_path": "%s" % value,
                                "space_token": space_token,
                            }
                    if key == "Protocol":
                        params["domains"]["lan"]["read"] = read_protocols.get(value, 0)
                        params["domains"]["wan"]["read"] = read_protocols.get(value, 0)
                        params["domains"]["lan"]["write"] = write_protocols.get(value, 0)
                        params["domains"]["wan"]["write"] = write_protocols.get(value, 0)
                        params["domains"]["lan"]["delete"] = write_protocols.get(value, 0)
                        params["domains"]["wan"]["delete"] = write_protocols.get(value, 0)
                        params["domains"]["wan"]["third_party_copy"] = write_protocols.get(value, 0)
                seProtocols[seName].append(params)
    log.debug("Accepted Dirac SEs: ", seProtocols)
    return S_OK(seProtocols)


def configHelper(voList):
    """
    A helper function to gather necessary Rucio client options from the CS.

    :param volist: list of VO names, or  a VO name  (str)
    :return: a dictionary of a form {vo: params, vo: params,}
    :rtype: dict
    """
    log = gLogger.getLocalSubLogger("RucioSynchronizerHelper")

    if isinstance(voList, str):
        voList = [voList]
    clientConfig = {}
    log.debug("VO list to consider for synchronization: ", voList)
    # locate RucioFileCatalog type in resources first
    result = gConfig.getSections("/Resources/FileCatalogs")
    catNames = []
    if result["OK"]:
        catalogs = result["Value"]
        log.debug("File catalogs defined in Resources", catalogs)
        for catalog in catalogs:
            result = gConfig.getOptionsDict(getCatalogPath(catalog))
            if result["OK"]:
                options = result["Value"]
                log.debug("Rucio Catalog candidate options", options)
                if options.get("Status", None) == "Active" and options.get("CatalogType", None) == "RucioFileCatalog":
                    catNames.append(catalog)
    else:
        log.error("No catalogs defined in Resources.")
        return S_ERROR("No catalogs defined in Resources.")

    log.info("Active FileCatalogs candidates of type RucioFileCatalog found in Resources:", catNames)
    # we found (possibly more that one) candidate, now we look for it in Operations
    # to find out which one is used by which VO. There can be only one
    # Rucio catalog per VO.

    for vo in voList:
        opHelper = Operations(vo=vo)
        result = opHelper.getSections("/Services/Catalogs")
        if result["OK"]:
            catSections = set(result["Value"])
        else:
            log.warn("No Services/Catalogs section in Operations, for ", "VO=%s (skipped)" % vo)
            continue

        selectedCatalog = list(catSections.intersection(catNames))

        if len(selectedCatalog) > 1:
            log.error(
                "VO %s: Services/Catalogs section mis-configured." " More that one Rucio file catalog",
                f"[VO: {vo}, Catalogs: {selectedCatalog}]",
            )
            continue

        if not selectedCatalog:
            log.warn("VO is not using RucioFileCatalog  (VO skipped)", "[VO: %s]" % vo)
            continue

        # check if the section name is in the catalog list to use.
        # if the list is not empty it has to contain the selected catalog.
        fileCatalogs = opHelper.getValue("/Services/Catalogs/CatalogList", [])

        if fileCatalogs and selectedCatalog[0] not in fileCatalogs:
            log.warn("VO is not using RucioFileCatalog - it is not in the catalog list", "[VO: %s]" % vo)
            continue
        # now collect Rucio specific parameters for the VO
        params = {}
        result = gConfig.getOptionsDict(getCatalogPath(selectedCatalog[0]))
        if result["OK"]:
            optDict = result["Value"]
            params["rucioHost"] = optDict.get("RucioHost", None)
            params["authHost"] = optDict.get("AuthHost", None)
            params["privilegedAccount"] = optDict.get("PrivilegedAccount", "root")
            clientConfig[vo] = params
            log.info("RSEs and users will be configured in Rucio for the VO:", vo)
        else:
            log.error(result["Message"])
    return clientConfig


class RucioSynchronizerAgent(AgentModule):
    """
    .. class::  RucioSynchronizerAgent

    Agent that synchronizes Dirac users and SEs to Rucio.
    """

    def initialize(self):
        """agent's initialisation

        :param self: self reference
        """
        self.log.info("Starting RucioSynchronizer")
        # CA location
        self.caCertPath = Locations.getCAsLocation()
        # configured VOs
        res = getVOs()
        if res["OK"]:
            voList = getVOs().get("Value", [])
        else:
            return S_ERROR(res["Message"])

        if isinstance(voList, str):
            voList = [voList]

        self.clientConfig = configHelper(voList)
        self.log.debug(" VO-specific Rucio Client config parameters: ", self.clientConfig)
        return S_OK()

    def execute(self):
        """
        Create RSEs in Rucio based on information in Dirac CS.

        :return: S_OK if all vital VO specific synchronisation succeeded, otherwise S_ERROR
        :rtype: dict
        """

        # for each VO: accumulate failed execution results with their messages
        gen = ((key, self.executeForVO(key)) for key in self.clientConfig)
        voRes = {key: result["Message"] for key, result in gen if not result["OK"]}

        if not voRes:
            return S_OK()
        else:
            message = "Synchronisation for at least one VO among eligible VOs was NOT successful."
            self.log.info(message)
            self.log.debug(voRes)
            return S_ERROR(message)

    def executeForVO(self, vo):
        """
        Execute one SE and user synchronisation cycle for a VO.

        :param str vo: Virtual organisation name.
        :return: S_OK or S_ERROR
        :rtype: dict
        """

        valid_protocols = ["srm", "gsiftp", "davs", "https", "root"]
        default_email = None
        try:
            try:
                client = Client(account="root", auth_type="userpass")
            except Exception as err:
                self.log.info("Login to Rucio as root with password failed. Will try host cert/key", str(err))
                certKeyTuple = Locations.getHostCertificateAndKeyLocation()
                if not certKeyTuple:
                    self.log.error("Hostcert/key location not set")
                    return S_ERROR("Hostcert/key location not set")
                hostcert, hostkey = certKeyTuple

                self.log.info("Logging in with a host cert/key pair:")
                self.log.debug("account: ", self.clientConfig[vo]["privilegedAccount"])
                self.log.debug("rucio host: ", self.clientConfig[vo]["rucioHost"])
                self.log.debug("auth  host: ", self.clientConfig[vo]["authHost"])
                self.log.debug("CA cert path: ", self.caCertPath)
                self.log.debug("Cert location: ", hostcert)
                self.log.debug("Key location: ", hostkey)
                self.log.debug("VO: ", vo)

                client = Client(
                    account=self.clientConfig[vo]["privilegedAccount"],
                    rucio_host=self.clientConfig[vo]["rucioHost"],
                    auth_host=self.clientConfig[vo]["authHost"],
                    ca_cert=self.caCertPath,
                    auth_type="x509",
                    creds={"client_cert": hostcert, "client_key": hostkey},
                    timeout=600,
                    user_agent="rucio-clients",
                    vo=vo,
                )

            self.log.info("Rucio client instantiated for VO:", vo)

            # Get the storage elements from Dirac Configuration and create them in Rucio
            newRSE = False
            self.log.info("Synchronizing SEs for VO ", vo)
            result = getStorageElements(vo)
            if result["OK"]:
                rses = [rse["rse"] for rse in client.list_rses()]
                for se in result["Value"]:
                    if se not in rses:
                        # The SE doesn't exist. Will create it
                        newRSE = True
                        self.log.info("Rucio Storage Element does not exist and will be created:", se)
                        try:
                            client.add_rse(rse=se, deterministic=True, volatile=False)
                        except Exception as err:
                            self.log.error("Cannot create RSE", f"[RSE: {se}, Error: {str(err)}]")
                            continue

                        # Add RSE attributes for the new RSE
                        ret = gConfig.getOptionsDict("Resources/FTSEndpoints/FTS3")
                        ftsList = ""
                        if ret["OK"]:
                            ftsList = ",".join(ret["Value"].values())
                        dictRSEAttributes = {"naming_convention": "BelleII", "ANY": True, "fts": ftsList}
                        for key in dictRSEAttributes:
                            self.log.info(
                                "Setting RSE attributes",
                                f"[RSE: {se}, Attr. name: {key}, Value: {dictRSEAttributes[key]}]",
                            )
                            client.add_rse_attribute(se, key, value=dictRSEAttributes[key])
                        client.set_local_account_limit("root", se, 100000000000000000)

                    # Create the protocols
                    try:
                        protocols = client.get_protocols(se)
                    except RSEProtocolNotSupported as err:
                        self.log.info("Cannot get protocols for", f"[RSE {se}, Error: {str(err)}]")
                        protocols = []
                    existing_protocols = []
                    for prot in protocols:
                        existing_protocols.append(
                            (str(prot["scheme"]), str(prot["hostname"]), str(prot["port"]), str(prot["prefix"]))
                        )
                    protocols_to_create = []
                    for params in result["Value"][se]:
                        prot = (
                            str(params["scheme"]),
                            str(params["hostname"]),
                            str(params["port"]),
                            str(params["prefix"]),
                        )
                        protocols_to_create.append(prot)
                        if prot not in existing_protocols and prot[0] in valid_protocols:
                            # The protocol defined in Dirac does not exist in Rucio. Will be created
                            self.log.info(
                                "Will create new protocol:",
                                "%s://%s:%s%s on %s"
                                % (params["scheme"], params["hostname"], params["port"], params["prefix"], se),
                            )
                            try:
                                client.add_protocol(rse=se, params=params)
                            except Duplicate as err:
                                self.log.info(
                                    "Protocol already exists on", "[RSE: {}, schema:{}]".format(se, params["scheme"])
                                )
                            except Exception as err:
                                self.log.error("Cannot create protocol on RSE", f"[RSE: {se}, Error: {str(err)}]")
                        else:
                            update = False
                            for protocol in protocols:
                                if prot == (
                                    str(protocol["scheme"]),
                                    str(protocol["hostname"]),
                                    str(protocol["port"]),
                                    str(protocol["prefix"]),
                                ):
                                    # Check if the protocol defined in Dirac has the same priority as the one defined in Rucio
                                    for domain in ["lan", "wan"]:
                                        for activity in ["read", "write", "delete"]:
                                            if (
                                                params["domains"][domain][activity]
                                                != protocol["domains"][domain][activity]
                                            ):
                                                update = True
                                                break

                                    if (
                                        params["domains"]["wan"]["third_party_copy"]
                                        != protocol["domains"]["wan"]["third_party_copy"]
                                    ):
                                        update = True
                                    if update:
                                        data = {
                                            "prefix": params["prefix"],
                                            "read_lan": params["domains"]["lan"]["read"],
                                            "read_wan": params["domains"]["wan"]["read"],
                                            "write_lan": params["domains"]["lan"]["write"],
                                            "write_wan": params["domains"]["wan"]["write"],
                                            "delete_lan": params["domains"]["lan"]["delete"],
                                            "delete_wan": params["domains"]["wan"]["delete"],
                                            "third_party_copy": params["domains"]["wan"]["write"],
                                        }
                                        self.log.info(
                                            "Will update protocol:",
                                            "%s://%s:%s%s on %s"
                                            % (
                                                params["scheme"],
                                                params["hostname"],
                                                params["port"],
                                                params["prefix"],
                                                se,
                                            ),
                                        )
                                        client.update_protocols(
                                            rse=se,
                                            scheme=params["scheme"],
                                            data=data,
                                            hostname=params["hostname"],
                                            port=params["port"],
                                        )
                    for prot in existing_protocols:
                        if prot not in protocols_to_create:
                            self.log.info("Will delete protocol:", f"{prot[0]}://{prot[1]}:{prot[2]}{prot[3]} on {se}")
                            client.delete_protocols(se, scheme=prot[0], hostname=prot[1], port=prot[2])
            else:
                self.log.error("Cannot get SEs:", result["Message"])

            # If new RSE added, add distances
            rses = [rse["rse"] for rse in client.list_rses()]
            if newRSE:
                self.log.info("Adding distances")
                for src_rse, dest_rse in permutations(rses, r=2):
                    try:
                        client.add_distance(src_rse, dest_rse, {"ranking": 1, "distance": 10})
                    except Exception as err:
                        self.log.error(
                            "Cannot add distance for",
                            f"Source RSE: {src_rse}, Dest RSE: {dest_rse}, Error:{str(err)}",
                        )

            # Collect the shares from Dirac Configuration and create them in Rucio
            self.log.info("Synchronizing shares")
            result = Operations().getOptionsDict("Production/SEshares")
            if result["OK"]:
                rseDict = result["Value"]
                for rse in rses:
                    try:
                        self.log.info("Setting productionSEshare for", "[RSE: %s : Share: %s", rse, rseDict.get(rse, 0))
                        client.add_rse_attribute(rse, "productionSEshare", rseDict.get(rse, 0))
                    except Exception as err:
                        self.log.error("Cannot create productionSEshare for RSE:", rse)
            else:
                self.log.error("Cannot get SEs", result["Message"])

            result = Operations().getSections("Shares")
            if result["OK"]:
                for dataLevel in result["Value"]:
                    result = Operations().getOptionsDict("Shares/%s" % dataLevel)
                    if not result["OK"]:
                        self.log.error("Cannot get SEs:" % result["Message"])
                        continue
                    rseDict = result["Value"]
                    for rse in rses:
                        try:
                            self.log.info("Setting", f"{dataLevel}Share for {rse} : {rseDict.get(rse, 0)}")
                            client.add_rse_attribute(rse, "%sShare" % dataLevel, rseDict.get(rse, 0))
                        except Exception as err:
                            self.log.error("Cannot create share:", "%sShare for %s", dataLevel, rse)
            else:
                self.log.error("Cannot get shares:", result["Message"])

            # Create the RSE attribute PrimaryDataSE and OccupancyLFN
            result = gConfig.getValue("Resources/StorageElementGroups/PrimarySEs")
            result = getStorageElements(vo)
            if result["OK"]:
                allSEs = result["Value"]
                primarySEs = resolveSEGroup("PrimarySEs", allSEs)
                self.log.info("Will set primarySEs flag to:", str(primarySEs))
                for rse in rses:
                    if rse in allSEs:
                        storage = StorageElement(rse)
                        if not storage.valid:
                            self.log.warn("Storage element is not valid. Skipped RSE:", rse)
                            continue
                        occupancyLFN = storage.options.get("OccupancyLFN")
                        try:
                            client.add_rse_attribute(rse, "OccupancyLFN", occupancyLFN)
                        except Exception as err:
                            self.log.error(
                                "Cannot create RSE attribute OccupancyLFN for", f"[RSE: {rse}, Error: {str(err)}]"
                            )
                    if rse in primarySEs:
                        try:
                            client.add_rse_attribute(rse, "PrimaryDataSE", True)
                        except Exception as err:
                            self.log.error(
                                "Cannot create RSE attribute PrimaryDataSE for",
                                f"[RSE: {rse}, Error: {str(err)}]",
                            )
                    else:
                        try:
                            client.delete_rse_attribute(rse, "PrimaryDataSE")
                        except RSEAttributeNotFound:
                            pass
                        except Exception as err:
                            self.log.error(
                                "Cannot remove RSE attribute PrimaryDataSE for",
                                f"[RSE: {rse}, Error: {str(err)}]",
                            )
            self.log.info("RSEs synchronized for VO: ", vo)

            # Collect the user accounts from Dirac Configuration and create user accounts in Rucio
            self.log.info("Synchronizing accounts for VO", vo)
            listAccounts = [str(acc["account"]) for acc in client.list_accounts()]
            listScopes = [str(scope) for scope in client.list_scopes()]
            dnMapping = {}
            diracUsers = getUsersInVO(vo)
            self.log.debug(" Will consider following Dirac users for", f"[VO: {vo}, Dirac users: {diracUsers}]")

            for account in diracUsers:
                dn = getUserOption(account, "DN")
                email = getUserOption(account, "Email")
                dnMapping[dn] = email
                if account not in listAccounts:
                    self.log.info("Will create account with associated DN ", f"[account: {account}, DN: {dn}]")
                    try:
                        client.add_account(account, "USER", email)
                        listAccounts.append(account)
                    except Exception as err:
                        self.log.error("Cannot create account", f"[account: {account}, Error: {str(err)}]")
                    try:
                        client.add_identity(account=account, identity=dn, authtype="X509", email=email, default=True)
                    except Exception as err:
                        self.log.error(
                            "Cannot add identity for account",
                            f"[Identity: dn={dn},  account:{account}, Error: {str(err)}]",
                        )
                        self.log.error(
                            " Account/identity skipped (it will not be created in Rucio)", f"[{account}/{dn}]"
                        )
                        continue
                    for rse in rses:
                        client.set_local_account_limit(account, rse, 1000000000000000)
                else:
                    try:
                        client.add_identity(account=account, identity=dn, authtype="X509", email=email, default=True)
                    except Duplicate:
                        pass
                    except Exception as err:
                        self.log.error(
                            "Cannot create identity for account",
                            f"[DN: {dn}, account: {account}, Error: {str(err)}]",
                        )
                scope = "user." + account
                if scope not in listScopes:
                    try:
                        self.log.info("Will create a scope", "[Scope:  %s]" % scope)
                        client.add_scope(account, scope)
                        self.log.info("Scope successfully added", "[Scope:  %s]" % scope)
                    except Exception as err:
                        self.log.error("Cannot create a scope", f"[Scope: {scope}, Error: {str(err)}]")

            # Collect the group accounts from Dirac Configuration and create service accounts in Rucio
            result = getGroupsForVO(vo)
            if result["OK"]:
                groups = result["Value"]
                self.log.debug(" Will consider following Dirac groups for", f"[{vo} VO: {groups}]")
            else:
                groups = []
                self.log.debug("No Dirac groups for", "%s VO " % vo)
                self.log.debug("No Rucio service accounts will be created")
            for group in groups:
                if group not in listAccounts:
                    self.log.info("Will create SERVICE account for Dirac group:", str(group))
                    try:
                        client.add_account(group, "SERVICE", None)
                        listAccounts.append(group)
                    except Exception as err:
                        self.log.error("Cannot create SERVICE account for", f"[group: {group}, Error: {str(err)}]")
                    for rse in rses:
                        client.set_local_account_limit(account, rse, 1000000000000000)

                for dn in getDNsInGroup(group):
                    try:
                        client.add_identity(
                            account=group, identity=dn, authtype="X509", email=dnMapping.get(dn, default_email)
                        )
                    except Duplicate:
                        pass
                    except Exception as err:
                        self.log.error(
                            "Cannot create identity for account",
                            f"[identity {dn}, account {group}, Error: {str(err)}]",
                        )
                        self.log.error(format_exc())

            # Collect the group accounts from Dirac Configuration and create service accounts in Rucio
            result = getHosts()
            if not result["OK"]:
                self.log.error("Cannot get host accounts:", "%s" % result["Message"])
            else:
                hosts = result["Value"]
                for host in hosts:
                    dn = getHostOption(host, "DN")
                    email = dnMapping.get(dn, default_email)
                    try:
                        client.add_identity(account="dirac_srv", identity=dn, authtype="X509", email=email)
                    except Duplicate:
                        pass
                    except Exception as err:
                        self.log.error(
                            "Cannot create identity for account dirac_srv:", f"[DN: {dn}, Error: {str(err)}]"
                        )
                        self.log.error(format_exc())

            return S_OK()
        except Exception as exc:
            self.log.exception("Synchronisation for VO failed. VO skipped ", "VO=%s" % vo, lException=exc)
            return S_ERROR(str(format_exc()))
