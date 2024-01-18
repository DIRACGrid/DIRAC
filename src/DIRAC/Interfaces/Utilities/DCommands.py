#!/usr/bin/env python
import glob
import json
import os.path
import random
import re
import stat
import time
import uuid
from configparser import ConfigParser, NoOptionError, NoSectionError

import DIRAC
import DIRAC.Core.Security.ProxyInfo as ProxyInfo
import DIRAC.FrameworkSystem.Client.ProxyGeneration as ProxyGeneration
from DIRAC import S_ERROR, S_OK, gConfig, gLogger
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.Core.Security import VOMS, Locations
from DIRAC.Core.Security.DiracX import addTokenToPEM
from DIRAC.Core.Security.Locations import getCAsLocation
from DIRAC.Core.Utilities.PrettyPrint import printTable
from DIRAC.FrameworkSystem.Client.BundleDeliveryClient import BundleDeliveryClient
from DIRAC.Resources.Catalog.FileCatalog import FileCatalog

# -----------------------------
# Proxy manipulation functions
# -----------------------------


def _getProxyLocation():
    return Locations.getProxyLocation()


def _getProxyInfo(proxyPath=False):
    if not proxyPath:
        proxyPath = _getProxyLocation()

    proxy_info = ProxyInfo.getProxyInfo(proxyPath, False)

    return proxy_info


# ----------------------------
# Output formatting utilities
# ----------------------------


def listFormatPretty(summaries, headers=None, sortKeys=None):
    records = []
    for _k, i in sortKeys:
        records.append([str(x) for x in summaries[i]])

    output = printTable(headers, records, numbering=False, printOut=False, columnSeparator="  ")
    return output


def listFormatCSV(summaries, headers=None, sortKeys=None):
    ret = ""
    for header in headers:
        ret += header + ","
    ret += "\n"

    if not sortKeys:
        sortKeys = map(lambda e: (None, e), range(len(summaries)))

    for _k, i in sortKeys:
        s = summaries[i]
        for i, header in enumerate(headers):
            ret += str(s[i]) + ","
        ret += "\n"
    return ret


def listFormatJSON(summaries, headers=None, sortKeys=None):
    l = []
    if not sortKeys:
        sortKeys = map(lambda e: (None, e), range(len(summaries)))

    for _k, i in sortKeys:
        s = summaries[i]
        d = {}
        for j, header in enumerate(headers):
            d[header] = s[j]
        l.append(d)

    return json.dumps(l)


class ArrayFormatter:
    fmts = {"csv": listFormatCSV, "pretty": listFormatPretty, "json": listFormatJSON}

    def __init__(self, outputFormat):
        self.outputFormat = outputFormat

    def listFormat(self, list_, headers, sort=None):
        if self.outputFormat not in self.fmts:
            return S_ERROR(
                f"ArrayFormatter: Output format not supported: {self.outputFormat} not in {self.fmts.keys()}"
            )

        if headers is None:
            if len(list_) == 0:
                return S_OK("")
            headers = range(list_)

        sortKeys = None
        if sort is not None:
            sortKeys = []
            for i, s in enumerate(list_):
                sortKeys.append((s[sort], i))
            sortKeys.sort()

        return self.fmts[self.outputFormat](list_, headers, sortKeys)

    def dictFormat(self, dict_, headers=None, sort=None):
        if headers is None:
            headers = dict_.keys()
        list_ = []
        for v in dict_.values():
            row = []
            for h in headers:
                row.append(v[h])
            list_.append(row)

        if sort is not None:
            sort = headers.index(sort)

        return self.listFormat(list_, headers, sort)


# -------------------------------
# DCommands configuration helpers
# -------------------------------


class DConfig:
    def __init__(self, configDir=None, configFilename="dcommands.conf"):
        try:
            self.config = ConfigParser(allow_no_value=True)
        except TypeError:
            self.config = ConfigParser()

        if not configDir:
            var = "DCOMMANDS_CONFIG_DIR"
            if var in os.environ:
                configDir = os.environ[var]
            else:
                configDir = os.path.expanduser("~/.dirac")

        self.configDir = configDir
        self.configFilename = configFilename
        self.configPath = os.path.join(self.configDir, self.configFilename)
        self.bootstrapFile()
        self.__buildSectionsAliases()

    def bootstrapFile(self):
        if not os.path.exists(self.configDir):
            os.mkdir(self.configDir)
            os.chmod(self.configDir, stat.S_IRWXU)
        elif not os.path.isdir(self.configDir):
            gLogger.error(f'"{self.configDir}" config dir is not a directory')
            DIRAC.exit(-1)
        elif not os.stat(self.configDir).st_mode != stat.S_IRWXU:
            gLogger.error(f'"{self.configDir}" config dir doesn\'t have correct permissions')
            DIRAC.exit(-1)
        if os.path.isfile(self.configPath):
            self.config.read(self.configPath)

    def __buildSectionsAliases(self):
        self.sectionsAliases = {}
        for section in self.config.sections():
            if self.config.has_option(section, "aliases"):
                aliases = self.config.get(section, "aliases")
                for alias in aliases.split(","):
                    self.sectionsAliases[alias.strip()] = section

        for section in self.config.sections():
            self.sectionsAliases[section] = section

    def sectionAliasName(self, alias):
        if alias not in self.sectionsAliases:
            return S_ERROR(f"DConfig section alias unknown: {alias}")

        return S_OK(self.sectionsAliases[alias])

    def write(self):
        with open(self.configPath, "w") as fh:
            self.config.write(fh)

    def has(self, section, option):
        return self.config.has_option(section, option)

    def get(self, section, option=None, defaultValue=None):
        value = defaultValue
        try:
            if not option:
                return S_OK(self.config.items(section))
            value = self.config.get(section, option)
        except NoOptionError:
            if defaultValue is None:
                return S_ERROR(
                    f'Option "{option}" missing in section "{section}" from configuration "{self.configPath}"'
                )
        except NoSectionError:
            if defaultValue is None:
                return S_ERROR(f'Section missing "{section}" from configuration "{self.configPath}"')
        return S_OK(value)

    def set(self, section, option=None, value=""):
        if section.lower() != "default" and not self.config.has_section(section):
            self.config.add_section(section)
        if option:
            self.config.set(section, option, value)

    def remove(self, section, option=None):
        if option:
            if not self.config.has_section(section):
                return S_ERROR(f'No such section "{section}" in file "{self.configFilename}"')
            self.config.remove_option(section, option)
        else:
            self.config.remove_section(section)

        return S_OK()

    def hasProfile(self, profile):
        return self.config.has_section(profile)

    def defaultProfile(self):
        retVal = self.get("global", "default_profile")
        if not retVal["OK"]:
            return None
        return retVal["Value"]

    def sections(self):
        return self.config.sections()

    def items(self, section):
        return self.config.items(section)

    def existsOrCreate(self, section, option, value):
        if self.config.has_section(section) and self.config.has_option(section, option):
            return False
        self.set(section, option, value)
        return True

    def fillMinimal(self):
        defaultGroup = ""
        # Try to find the default user group
        # todo: dirac-proxy-info contains the user group, use this
        resultInfo = ProxyInfo.getProxyInfo()
        if resultInfo["OK"]:
            userName = resultInfo["Value"].get("username")
            if userName:
                result = Registry.findDefaultGroupForUser(userName)
                if result["OK"]:
                    defaultGroup = result["Value"]
            if not defaultGroup:
                defaultGroup = resultInfo["Value"].get("group")
        if not defaultGroup:
            defaultGroup = "dirac_user"
        modified = False
        modified |= self.existsOrCreate("global", "default_profile", defaultGroup)
        modified |= self.existsOrCreate(defaultGroup, "group_name", defaultGroup)
        modified |= self.existsOrCreate(defaultGroup, "home_dir", "/")
        modified |= self.existsOrCreate(defaultGroup, "default_se", "DIRAC-USER")

        return modified


def createMinimalConfig(configDir=os.path.expanduser("~/.dirac"), configFilename="dcommands.conf"):
    dconfig = DConfig(configDir, configFilename)

    modified = dconfig.fillMinimal()

    if modified:
        dconfig.write()


# -------------------------
# DCommands Session helpers
# -------------------------


class DSession(DConfig):
    __ENV_SECTION = "session:environment"

    @classmethod
    def sessionFilePrefix(cls):
        return f"dsession.{uuid.getnode():x}"

    @classmethod
    def sessionFilename(cls, pid):
        return cls.sessionFilePrefix() + ".%d" % (pid,)

    def __init__(self, profileName=None, config=None, sessionDir=None, pid=None):
        if not config:
            config = DConfig()

        if not profileName:
            proxyPath = _getProxyLocation()
            if not proxyPath:
                gLogger.error("No proxy found")
                return None

            retVal = _getProxyInfo(proxyPath)
            if not retVal["OK"]:
                raise Exception(retVal["Message"])
            proxyInfo = retVal["Value"]
            groupName = proxyInfo.get("group")
            sections = config.sections()
            for s in sections:
                if config.has(s, "group_name") and config.get(s, "group_name")["Value"] == groupName:
                    profileName = s
                    break
            if not profileName:
                if not groupName:
                    raise Exception("cannot guess profile defaults without a DIRAC group in Proxy")
                profileName = "__guessed_profile__"
                userName = proxyInfo.get("username")
                gLogger.warn(f"No config section found for {groupName}, using default profile.")
                guessConfigFromCS(config, profileName, userName, groupName)

        self.origin = config
        modified = self.origin.fillMinimal()
        if modified:
            self.origin.write()

        self.pid = pid
        if not self.pid:
            self.pid = os.getppid()

        if not sessionDir:
            sessionDir = self.origin.configDir

        super().__init__(sessionDir, self.sessionFilename(self.pid))

        self.__cleanSessionDirectory()

        oldProfileName = self.getEnv("profile_name", "")["Value"]
        profileName = profileName or oldProfileName or self.origin.defaultProfile()
        retVal = self.origin.sectionAliasName(profileName)
        if not retVal["OK"]:
            gLogger.error(retVal["Message"])
            DIRAC.exit(-1)
        self.profileName = retVal["Value"]

        if not os.path.isfile(self.configPath) or self.profileName != oldProfileName:
            self.__clearEnv()
            # set default common options from section [global]
            self.copyProfile("global")
            # overwrite with options from profile section
            self.copyProfile()
            # add profile name
            self.setEnv("profile_name", self.profileName)
            # set working directory option
            self.setCwd(self.homeDir())

    def __cleanSessionDirectory(self):
        def pid_exists(pid):
            try:
                os.kill(pid, 0)
            except OSError as _err:
                # errno.EPERM would denote a process belonging to someone else
                # so we consider it inexistent
                # return _err.errno == errno.EPERM
                return False
            return True

        sessionPat = "^" + self.sessionFilePrefix() + r"\.(?P<pid>[0-9]+)$"
        sessionRe = re.compile(sessionPat)
        for f in os.listdir(self.configDir):
            m = sessionRe.match(f)
            if m is not None:
                pid = int(m.group("pid"))

                # delete session files for non running processes
                if not pid_exists(pid):
                    os.unlink(os.path.join(self.configDir, f))

    def getEnv(self, option, defaultValue=None):
        return self.get(DSession.__ENV_SECTION, option, defaultValue)

    def listEnv(self):
        return self.get(DSession.__ENV_SECTION)

    def setEnv(self, option, value):
        self.set(DSession.__ENV_SECTION, option, value)

    def unsetEnv(self, option):
        return self.remove(DSession.__ENV_SECTION, option)

    def __clearEnv(self):
        self.config.remove_section(DSession.__ENV_SECTION)

    def copyProfile(self, profileName=None):
        for o, v in self.origin.items(profileName or self.profileName):
            self.setEnv(o, v)

    def homeDir(self):
        return self.getEnv("home_dir", "/")["Value"]

    def getCwd(self):
        return self.getEnv("cwd", self.homeDir())["Value"]

    def setCwd(self, value):
        self.setEnv("cwd", value)

    def getReplicationSEs(self):
        replication_scheme = self.getEnv("replication_scheme", "all( )")["Value"]
        replication_ses = self.getEnv("replication_ses", "")["Value"]

        if not replication_ses:
            return []

        replication_ses = replication_ses.split(",")

        def randomSEs(num):
            random.shuffle(replication_ses)
            return replication_ses[0:num]

        schemes = {
            "all": lambda: replication_ses,
            "first": lambda num: replication_ses[0:num],
            "random": randomSEs,
        }

        return eval(replication_scheme, schemes)

    def getJDL(self):
        return self.getEnv("jdl", "")["Value"]

    def proxyInfo(self, proxyPath=None):
        return _getProxyInfo(proxyPath)

    def proxyIsValid(self, timeLeft=60):
        proxy_path = _getProxyLocation()
        if not proxy_path:
            return False

        retVal = self.proxyInfo(proxy_path)
        if not retVal["OK"]:
            return False

        pi = retVal["Value"]

        timeLeft = max(timeLeft, 0)

        retVal = self.getEnv("group_name")
        if not retVal["OK"]:
            return False
        group_name = retVal["Value"]

        return pi["secondsLeft"] > timeLeft and pi["validGroup"] and pi["group"] == group_name

    def checkCAs(self):
        """
        checks if CRLs are up to date and updates them if necessary
        """
        caDir = getCAsLocation()
        if not caDir:
            gLogger.warn("No valid CA dir found.")
            return
        # In globus standards .r0 files are CRLs. They have the same names of the CAs but diffent file extension
        searchExp = os.path.join(caDir, "*.r0")
        crlList = glob.glob(searchExp)
        if not crlList:
            gLogger.warn(f"No CRL files found for {searchExp}. Abort check of CAs")
            return
        newestFPath = max(crlList, key=os.path.getmtime)
        newestFTime = os.path.getmtime(newestFPath)
        if newestFTime > (time.time() - (2 * 24 * 3600)):
            # At least one of the files has been updated in the last 2 days
            return S_OK()
        if not os.access(caDir, os.W_OK):
            gLogger.error("Your CRLs appear to be outdated, but you have no access to update them.")
            # Try to continue anyway...
            return S_OK()
        # Update the CAs & CRLs
        gLogger.notice("Your CRLs appear to be outdated; attempting to update them...")
        bdc = BundleDeliveryClient()
        res = bdc.syncCAs()
        if not res["OK"]:
            gLogger.error("Failed to update CAs", res["Message"])
        res = bdc.syncCRLs()
        if not res["OK"]:
            gLogger.error("Failed to update CRLs", res["Message"])
        # Continue even if the update failed...
        return S_OK()

    def proxyInit(self):
        params = ProxyGeneration.CLIParams()
        retVal = self.getEnv("group_name")
        if not retVal["OK"]:
            raise Exception(retVal["Message"])

        params.diracGroup = retVal["Value"]

        result = ProxyGeneration.generateProxy(params)
        if not result["OK"]:
            raise Exception(result["Message"])
        filename = result["Value"]

        self.checkCAs()

        try:
            self.addVomsExt(filename)
        except:
            # silently skip VOMS errors
            pass

        if not (result := addTokenToPEM(filename, params.diracGroup))["OK"]:  # pylint: disable=unsubscriptable-object
            raise Exception(result["Message"])  # pylint: disable=unsubscriptable-object

    def addVomsExt(self, proxy):
        retVal = self.getEnv("group_name")
        if not retVal["OK"]:
            raise Exception(retVal["Message"])

        group = retVal["Value"]
        vomsAttr = Registry.getVOMSAttributeForGroup(group)
        if not vomsAttr:
            raise Exception(f"Requested adding a VOMS extension but no VOMS attribute defined for group {group}")

        result = VOMS.VOMS().setVOMSAttributes(proxy, attribute=vomsAttr, vo=Registry.getVOForGroup(group))
        if not result["OK"]:
            raise Exception(
                f"Could not add VOMS extensions to the proxy\nFailed adding VOMS attribute: {result['Message']}"
            )

        chain = result["Value"]
        chain.dumpAllToFile(proxy)

    def getUserName(self):
        proxyPath = _getProxyLocation()
        if not proxyPath:
            return S_ERROR("no proxy location")
        retVal = self.proxyInfo()
        if not retVal["OK"]:
            return retVal

        return S_OK(retVal["Value"]["username"])


def guessConfigFromCS(config, section, userName, groupName):
    """
    try to guess best DCommands default values from Configuration Server
    """
    # write group name
    config.set(section, "group_name", groupName)

    # guess FileCatalog home directory
    vo = gConfig.getValue(f"/Registry/Groups/{groupName}/VO")
    firstLetter = userName[0]
    homeDir = f"/{vo}/user/{firstLetter}/{userName}"

    config.set(section, "home_dir", homeDir)

    # try to guess default SE DIRAC name
    voDefaultSEName = f"VO_{vo.upper()}_DEFAULT_SE"
    voDefaultSEName = voDefaultSEName.replace(".", "_")
    voDefaultSEName = voDefaultSEName.replace("-", "_")
    try:
        voDefaultSEHost = os.environ[voDefaultSEName]
    except KeyError:
        voDefaultSEHost = None
    if voDefaultSEHost:
        retVal = gConfig.getSections("/Resources/StorageElements")
        if retVal["OK"]:
            defaultSESite = None
            for seSite in retVal["Value"]:
                # look for a SE with same host name
                host = gConfig.getValue(f"/Resources/StorageElements/{seSite}/AccessProtocol.1/Host")
                if host and host == voDefaultSEHost:
                    # check if SE has rw access
                    retVal = gConfig.getOptionsDict(f"/Resources/StorageElements/{seSite}")
                    if retVal["OK"]:
                        od = retVal["Value"]
                        r = "ReadAccess"
                        w = "WriteAccess"
                        active = "Active"
                        ok = r in od and od[w] == active
                        ok &= w in od and od[w] == active

                        if ok:
                            defaultSESite = seSite
                    # don't check other SE sites
                    break

            if defaultSESite:
                # write to config
                config.set(section, "default_se", defaultSESite)


# -------------------------
# DCommands FC/LFN helpers
# -------------------------


def createCatalog():
    return FileCatalog()


class DCatalog:
    """
    DIRAC File Catalog helper
    """

    def __init__(self):
        self.catalog = createCatalog()

    def isDir(self, path):
        result = self.catalog.isDirectory(path)
        if result["OK"]:
            if result["Value"]["Successful"]:
                if result["Value"]["Successful"][path]:
                    return True
        return False

    def isFile(self, path):
        result = self.catalog.isFile(path)
        if result["OK"] and path in result["Value"]["Successful"] and result["Value"]["Successful"][path]:
            return True
        return False

    def getMeta(self, path):
        if self.isDir(path):
            return self.catalog.getDirectoryUserMetadata(path)
        return self.catalog.getFileUserMetadata(path)

    def findFilesByMetadata(self, metaDict, path):
        return self.catalog.findFilesByMetadata(metaDict, path)


def pathFromArgument(session, arg):
    path = os.path.normpath(arg)
    if not os.path.isabs(path):
        path = os.path.normpath(os.path.join(session.getCwd(), path))
    return path


def pathFromArguments(session, args):
    ret = []

    for arg in args:
        ret.append(pathFromArgument(session, arg))

    return ret or [session.getCwd()]
