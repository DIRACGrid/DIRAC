""" This helper looks in the /Operations section of the CS, considering its specific nature:
    the /Operations section is designed in a way that each configuration can be specific to a Setup,
    while maintaining a default.

    So, for example, given the following /Operations section::

      Operations/
          Defaults/
                someOption = someValue
                aSecondOption = aSecondValue
          specificVo/
              someSection/
                  someOption = someValueInVO

    The following calls would give different results based on the setup::

      Operations().getValue('someSection/someOption')
        - someValueInVO if we are in 'specificVo' vo
        - someValue if we are in any other VO

    It becomes then important for the Operations() objects to know the VO name
    for which we want the information, and this can be done in the following ways.

    1. by specifying the VO name directly::

         Operations(vo=anotherVOName).getValue('someSectionName/someOptionX')

    2. by give a group name::

         Operations(group=thisIsAGroupOfVO_X).getValue('someSectionName/someOptionX')

    3. if no VO nor group is provided, the VO will be guessed from the proxy,
    but this works iff the object is instantiated by a proxy (and not, e.g., using a server certificate)

"""
import _thread

from diraccfg import CFG

from DIRAC import S_ERROR, S_OK
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
from DIRAC.ConfigurationSystem.Client.Helpers import CSGlobals, Registry
from DIRAC.Core.Security.ProxyInfo import getVOfromProxyGroup
from DIRAC.Core.Utilities import LockRing
from DIRAC.Core.Utilities.DErrno import ESECTION


class Operations:
    """Operations class

    The /Operations CFG section is maintained in a cache by an Operations object
    """

    __cache = {}
    __cacheVersion = 0
    __cacheLock = LockRing.LockRing().getLock()

    def __init__(self, vo=False, group=False, setup=False):
        """c'tor

        Setting some defaults
        """
        self.__uVO = vo
        self.__uGroup = group
        self.__vo = False
        self.__discoverSettings()

    def __discoverSettings(self):
        """Discovers the vo and the setup"""
        # Set the VO
        globalVO = CSGlobals.getVO()
        if globalVO:
            self.__vo = globalVO
        elif self.__uVO:
            self.__vo = self.__uVO
        elif self.__uGroup:
            self.__vo = Registry.getVOForGroup(self.__uGroup)
            if not self.__vo:
                self.__vo = False
        else:
            result = getVOfromProxyGroup()
            if result["OK"]:
                self.__vo = result["Value"]

    def __getCache(self):
        Operations.__cacheLock.acquire()
        try:
            currentVersion = gConfigurationData.getVersion()
            if currentVersion != Operations.__cacheVersion:
                Operations.__cache = {}
                Operations.__cacheVersion = currentVersion

            cacheKey = (self.__vo,)
            if cacheKey in Operations.__cache:
                return Operations.__cache[cacheKey]

            mergedCFG = CFG()

            for path in self.__getSearchPaths():
                pathCFG = gConfigurationData.mergedCFG[path]
                if pathCFG:
                    mergedCFG = mergedCFG.mergeWith(pathCFG)

            Operations.__cache[cacheKey] = mergedCFG

            return Operations.__cache[cacheKey]
        finally:
            try:
                Operations.__cacheLock.release()
            except _thread.error:
                pass

    def __getSearchPaths(self):
        paths = ["/Operations/Defaults"]
        if not self.__vo:
            globalVO = CSGlobals.getVO()
            if not globalVO:
                return paths
            self.__vo = CSGlobals.getVO()
        paths.append(f"/Operations/{self.__vo}/")
        return paths

    def getValue(self, optionPath, defaultValue=None):
        return self.__getCache().getOption(optionPath, defaultValue)

    def __getCFG(self, sectionPath):
        cacheCFG = self.__getCache()
        section = cacheCFG.getRecursive(sectionPath)
        if not section:
            return S_ERROR(ESECTION, f"{sectionPath} in Operations does not exist")
        sectionCFG = section["value"]
        if isinstance(sectionCFG, str):
            return S_ERROR(f"{sectionPath} in Operations is not a section")
        return S_OK(sectionCFG)

    def getSections(self, sectionPath, listOrdered=False):
        result = self.__getCFG(sectionPath)
        if not result["OK"]:
            return result
        sectionCFG = result["Value"]
        return S_OK(sectionCFG.listSections(listOrdered))

    def getOptions(self, sectionPath, listOrdered=False):
        result = self.__getCFG(sectionPath)
        if not result["OK"]:
            return result
        sectionCFG = result["Value"]
        return S_OK(sectionCFG.listOptions(listOrdered))

    def getOptionsDict(self, sectionPath):
        result = self.__getCFG(sectionPath)
        if not result["OK"]:
            return result
        sectionCFG = result["Value"]
        data = {}
        for opName in sectionCFG.listOptions():
            data[opName] = sectionCFG[opName]
        return S_OK(data)

    def getMonitoringBackends(self, monitoringType=None):
        """
        Chooses the type of backend to use (Monitoring and/or Accounting) depending on the MonitoringType.
        If a flag for the monitoringType specified is set, it will enable monitoring according to it,
        otherwise it will use the `Default` value (Accounting set as default).

        :param string MonitoringType: monitoring type to specify
        """
        if monitoringType and self.getValue(f"MonitoringBackends/{monitoringType}"):
            return self.getValue(f"MonitoringBackends/{monitoringType}", [])
        else:
            return self.getValue("MonitoringBackends/Default", ["Accounting"])
