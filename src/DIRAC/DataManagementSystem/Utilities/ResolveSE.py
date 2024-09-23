""" This module allows to resolve output SEs for Job based
on SE and site/country association
"""

from random import shuffle

from DIRAC import gLogger, gConfig
from DIRAC.Core.Utilities.SiteSEMapping import getSEsForSite
from DIRAC.DataManagementSystem.Utilities.DMSHelpers import resolveSEGroup

sLog = gLogger.getSubLogger(__name__)


def _setLocalFirst(seList, localSEs):
    """return a shuffled list of SEs from seList, localSEs being first."""
    # Make a copy not to change the original order
    seList = list(seList)
    shuffle(seList)
    # localSEs are put first in the list
    return sorted(seList, key=lambda x: x not in localSEs)


def getDestinationSEList(outputSE, site, outputmode="Any"):
    """Evaluate the output SE list from a workflow and return the concrete list
    of SEs to upload output data. The resolution order goes as follow:

    * outputSE as a normal StorageElement
    * outputSE as an alias of one SE defined in the ``site`` AssociatedSEs (return local first)
    * outputSE as an alias of multiple SE (local SE should come first)
    * outputSE as a StorageElementGroup

    Moreover, if output mode is `Local`:

    * return ONLY local SE within the SEGroup if they exist (i.e. in the ``<site>/SE>`` config)
    * look at associated countries and countries association


    :param str outputSE: name of the SE or SEGroup we want to resolve
    :param str site: site on which we are running
    :param str outputmode: (default "Any") resolution mode

    :returns: list of string

    :raises:
        RuntimeError if anything is wrong

    """

    if not outputSE:
        return []

    if outputmode.lower() not in ("any", "local"):
        raise RuntimeError("Unexpected outputmode")

    # Add output SE defined in the job description
    sLog.info("Resolving workflow output SE description", str(outputSE))

    # Check if the SE is defined explicitly for the site
    prefix = site.split(".")[0]
    country = site.split(".")[-1]

    # Concrete SE name
    result = gConfig.getOptions(f"/Resources/StorageElements/{outputSE}")
    if result["OK"]:
        sLog.info("Found concrete SE", str(outputSE))
        return [outputSE]

    # Get local SEs
    localSEs = getSEsForSite(site)
    if not localSEs["OK"]:
        raise RuntimeError(localSEs["Message"])
    localSEs = localSEs["Value"]
    sLog.verbose("Local SE list is:", ", ".join(localSEs))
    # There is an alias defined for this Site
    associatedSEs = gConfig.getValue(f"/Resources/Sites/{prefix}/{site}/AssociatedSEs/{outputSE}", [])
    if associatedSEs:
        associatedSEs = _setLocalFirst(associatedSEs, localSEs)
        sLog.info("Found associated SE for site", f"{associatedSEs} associated to {site}")
        return associatedSEs

    groupSEs = resolveSEGroup(outputSE)
    if not groupSEs:
        raise RuntimeError(f"Failed to resolve SE {outputSE}")
    sLog.verbose("Group SE list is:", str(groupSEs))

    # Find a local SE or an SE considered as local because the country is associated to it
    if outputmode.lower() == "local":
        # First, check if one SE in the group is local
        ses = list(set(localSEs) & set(groupSEs))
        if ses:
            sLog.info("Found eligible local SE", str(ses))
            return ses

        # Final check for country associated SE
        assignedCountry = country
        while True:
            # check if country is already one with associated SEs
            section = f"/Resources/Countries/{assignedCountry}/AssociatedSEs/{outputSE}"
            associatedSEs = gConfig.getValue(section, [])
            if associatedSEs:
                associatedSEs = _setLocalFirst(associatedSEs, localSEs)
                sLog.info("Found associated SEs", f"{associatedSEs} in {section}")
                return associatedSEs

            opt = gConfig.getOption(f"/Resources/Countries/{assignedCountry}/AssignedTo")
            if opt["OK"] and opt["Value"]:
                assignedCountry = opt["Value"]
            else:
                # No associated SE and no assigned country, give up
                raise RuntimeError(
                    f"Could not establish associated SE nor assigned country for country {assignedCountry}"
                )

    # For collective Any and All modes return the whole group
    # Make sure that local SEs are passing first
    orderedSEs = _setLocalFirst(groupSEs, localSEs)
    sLog.info("Found SEs, local first:", str(orderedSEs))
    return orderedSEs
