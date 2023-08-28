import os
import pytest
import tempfile


from DIRAC.tests.Utilities.utils import generateDIRACConfig

from DIRAC.DataManagementSystem.Utilities.ResolveSE import getDestinationSEList
from DIRAC.DataManagementSystem.Utilities.DMSHelpers import resolveSEGroup

from DIRAC import gLogger

gLogger.setLevel("DEBUG")


CFG_CONTENT = """
Resources
{
    StorageElements
    {
        LogSE
        {
        }
        CERN-BUFFER
        {
        }
        CERN-FAILOVER
        {
        }
        CERN-DST
        {
        }
        CNAF-BUFFER
        {
        }
        CNAF-FAILOVER
        {
        }
        CNAF-DST
        {
        }
        GRIDKA-BUFFER
        {
        }
        GRIDKA-FAILOVER
        {
        }
        GRIDKA-DST
        {
        }
    }
    StorageElementGroups
    {
        # Define a StorageElementGroup which is not
        # overwriten in any associatedSEs
        Tier1-Failover = CERN-FAILOVER, CNAF-FAILOVER

        # Define a SEGroup which will be
        # overwriten at CERN only
        Tier1-DST = CERN-DST, CNAF-DST
    }
    Sites
    {
        LCG
        {
            LCG.CERN.cern
            {
            # Local SEs
            SE = CERN-BUFFER, CERN-LogSE, CERN-FAILOVER, CNAF-DST

            AssociatedSEs
            {

                # This should have no impact because
                # LogSE exists as an SE, so we do not
                # check associated SE

                LogSE = CERN-LogSE

                # Here define a name that is used as an alias

                LocalLogSE = CERN-LogSE

                # Tier1-Buffer is defined as associated site
                # for all Sites.
                # It's not a real use case.
                # We would normally define a StorageElementGroup
                # and overwrite it if needed in some sites
                # (like what we do for Tier1-DST)
                Tier1-Buffer = CERN-BUFFER, CNAF-BUFFER

                # Overwite the definition of the Tier1-DST group
                # for this site only
                Tier1-DST = CERN-DST
            }
        }

        LCG.CNAF.it
        {
            SE = CNAF-BUFFER, CNAF-FAILOVER, CNAF-DST
            AssociatedSEs
            {
                # We do NOT define CNAF-LogSE as local, yet it is still returned
                # when using ``outputmode = local`` because ``local`` only impacts
                # ``StorageElementGroups``
                LocalLogSE = CNAF-LogSE

                # We anyway expect CNAF-BUFFER to be returned first, as it is local
                Tier1-Buffer = CERN-BUFFER, CNAF-BUFFER
            }
        }

        # Do not update anything for this site
        LCG.IN2P3.fr
        {
        }

        # Do not update anything for this site
        # but define some aliases in the ``de`` country
        LCG.GRIDKA.de
        {
        }
      }
    }

    # Countries are taking into account only when using ``Local`` mode
    Countries
    {
        pl
        {
            # Associate Polish sites to german sites
            AssignedTo = de
        }
        de
        {
            AssociatedSEs
            {

                # This alias will never be used
                # because ``LocalLogSE`` is not a
                # ``StorageElementGroup``, so the resolution
                # does not take place, even when used with ``Local``
                LocalLogSE = GRIDKA-LogSE

                # As for LocalLogSE, we will never reach that
                # alias
                Tier1-Buffer = GRIDKA-BUFFER

                Tier1-DST = GRIDKA-DST

            }
        }
    }
}
"""


@pytest.fixture(scope="module", autouse=True)
def loadCS():
    """Load the CFG_CONTENT as a DIRAC Configuration for this module"""
    with generateDIRACConfig(CFG_CONTENT, "test_resolveSE.cfg"):
        yield


def test_directSEName():
    """Should return the LogSE as is, full stop"""
    # Asking for a specific SE should return that SE, no matter what
    for site in ("LCG.CERN.cern", "LCG.CNAF.it", "LCG.IN2P3.fr", "LCG.GRIDKA.de", "LCG.NCBJ.pl", "AnySite"):
        assert getDestinationSEList("LogSE", site) == ["LogSE"]
        assert getDestinationSEList("LogSE", site, outputmode="Local") == ["LogSE"]


def test_emptyInput():
    """Should return empty list"""

    assert getDestinationSEList("", "site") == []
    assert getDestinationSEList("", "site", outputmode="Local") == []
    assert getDestinationSEList([], "site") == []
    assert getDestinationSEList([], "site", outputmode="Local") == []


def test_directSEName_redefined():
    """Redifining an existing SEName has no impact"""
    # CERN redefines LogSE in the associatedSEs
    # but it should be ignored
    assert getDestinationSEList("LogSE", "LCG.CERN.cern") == ["LogSE"]
    assert getDestinationSEList("LogSE", "LCG.CERN.cern", outputmode="Local") == ["LogSE"]


def test_associatedSE_singleSE():
    """Map a given name to an SE in the sites definition"""
    # CERN defines LocalLogSE as "CERN-LogSE"
    assert getDestinationSEList("LocalLogSE", "LCG.CERN.cern") == ["CERN-LogSE"]
    assert getDestinationSEList("LocalLogSE", "LCG.CERN.cern", outputmode="Local") == ["CERN-LogSE"]
    # CNAF defines LocalLogSE as "CNAF-LogSE
    assert getDestinationSEList("LocalLogSE", "LCG.CNAF.it") == ["CNAF-LogSE"]
    # CNAF-LogSE is NOT defined as local, but it is still returned as
    # ``Local`` only impacts StorageElementGroups resolution
    assert getDestinationSEList("LocalLogSE", "LCG.CNAF.it", outputmode="Local") == ["CNAF-LogSE"]

    # LocalLogSE does not exist for these sites

    for site in ("LCG.IN2P3.fr", "LCG.GRIDKA.de", "LCG.NCBJ.pl"):
        with pytest.raises(RuntimeError):
            assert getDestinationSEList("LocalLogSE", site)

    # LocalLogSE is NOT a StorageElementGroup, so ``Local``
    # will not help, even if ``LocalLogSE`` is defined in the
    # ``de`` country
    # NOTE: That is quite counter intuitive and could be revisited
    for site in ("LCG.IN2P3.fr", "LCG.GRIDKA.de", "LCG.NCBJ.pl"):
        with pytest.raises(RuntimeError):
            assert getDestinationSEList("LocalLogSE", site, outputmode="Local")


def test_associatedSE_group():
    """Map a given name to multiple SEs. The order should be different
    because the local storages are returned first"""

    # CERN defines Tier1-Buffer as CERN-BUFFER, CNAF-BUFFER
    assert getDestinationSEList("Tier1-Buffer", "LCG.CERN.cern") == ["CERN-BUFFER", "CNAF-BUFFER"]
    # We could expect ``Local`` to reduce the output to CERN-BUFFER, but it does not
    # because Tier1-Buffer is NOT a StorageElement
    assert getDestinationSEList("Tier1-Buffer", "LCG.CERN.cern", outputmode="Local") == ["CERN-BUFFER", "CNAF-BUFFER"]

    # CNAF defines Tier1-Buffer as CERN-BUFFER, CNAF-BUFFER but the order is changed
    # because local SEs go first
    assert getDestinationSEList("Tier1-Buffer", "LCG.CNAF.it") == ["CNAF-BUFFER", "CERN-BUFFER"]
    # Same as CERN, with local SEs first
    assert getDestinationSEList("Tier1-Buffer", "LCG.CNAF.it") == ["CNAF-BUFFER", "CERN-BUFFER"]

    # Tier1-Buffer does not exist for these sites
    for site in ("LCG.IN2P3.fr", "LCG.GRIDKA.de", "LCG.NCBJ.pl"):
        with pytest.raises(RuntimeError):
            assert getDestinationSEList("Tier1-Buffer", site)

    # Tier1-Buffer does not exist for these sites, and since it is not
    # a StorageElementGroup, we do not do the country resolution
    for site in ("LCG.IN2P3.fr", "LCG.GRIDKA.de", "LCG.NCBJ.pl"):
        with pytest.raises(RuntimeError):
            assert getDestinationSEList("Tier1-Buffer", site, outputmode="Local")


def test_seGroup():
    """Test resolving a StorageElementGroup not redefined anywhere"""

    # Tier1-Failover is not redifined anywhere

    # Retrieve the full list, sorting local SEs first
    assert getDestinationSEList("Tier1-Failover", "LCG.CERN.cern") == ["CERN-FAILOVER", "CNAF-FAILOVER"]
    assert getDestinationSEList("Tier1-Failover", "LCG.CNAF.it") == ["CNAF-FAILOVER", "CERN-FAILOVER"]

    # In ``Local`` mode, we ONLY get local SE
    assert getDestinationSEList("Tier1-Failover", "LCG.CERN.cern", outputmode="Local") == ["CERN-FAILOVER"]
    assert getDestinationSEList("Tier1-Failover", "LCG.CNAF.it", outputmode="Local") == ["CNAF-FAILOVER"]

    # Here we get the full Tier1-Failover list
    # We have to compare with ``sorted`` because the order is shuffled
    for site in ("LCG.IN2P3.fr", "LCG.GRIDKA.de", "LCG.NCBJ.pl"):
        assert sorted(getDestinationSEList("Tier1-Failover", site)) == sorted(["CERN-FAILOVER", "CNAF-FAILOVER"])

    # When using the ``Local`` mode, we get an error because none of
    # the SE is local to any of the site.
    for site in ("LCG.IN2P3.fr", "LCG.GRIDKA.de", "LCG.NCBJ.pl"):
        with pytest.raises(RuntimeError):
            getDestinationSEList("Tier1-Failover", site, outputmode="Local")


def test_seGroup_overwrite():
    """Test resolving a StorageElementGroup which is redefined in some places"""

    # CERN redefineds Tier1-DST
    assert getDestinationSEList("Tier1-DST", "LCG.CERN.cern") == ["CERN-DST"]
    # CERN-DST is local to CERN, so the same when using ``Local`` mode
    assert getDestinationSEList("Tier1-DST", "LCG.CERN.cern", outputmode="Local") == ["CERN-DST"]

    # CNAF does NOT redefine Tier1-DST, but it puts local SE first
    assert getDestinationSEList("Tier1-DST", "LCG.CNAF.it") == ["CNAF-DST", "CERN-DST"]
    # CNAF-DST is local to CNAF, so get only that if using ``Local`` mode
    assert getDestinationSEList("Tier1-DST", "LCG.CNAF.it", outputmode="Local") == ["CNAF-DST"]

    # Sites do not redefine Tier1-DST, so we get the full
    # Tier1-DST list in a random order
    for site in ("LCG.IN2P3.fr", "LCG.GRIDKA.de", "LCG.NCBJ.pl"):
        assert sorted(getDestinationSEList("Tier1-DST", site)) == sorted(["CNAF-DST", "CERN-DST"])

    # There are no SE in Tier1-DST which are ``Local`` to IN2P3,
    # and IN2P3 is not associated to any country,
    # so we don't find anything
    with pytest.raises(RuntimeError):
        getDestinationSEList("Tier1-DST", "LCG.IN2P3.fr", outputmode="Local")

    # Here we redefine the Tier1-DST at the country level:
    # de has GRIDKA-DST as Tier1-DST
    # pl points to de
    for site in ("LCG.GRIDKA.de", "LCG.NCBJ.pl"):
        assert getDestinationSEList("Tier1-DST", site, outputmode="Local") == ["GRIDKA-DST"]


def test_compat_resolveSEGroup():
    """We want to make sure that resolveSEGroup produces the same output
    when we do not overwrite the SEGroup definition and use the default outputMode
    """

    for site in ("LCG.CERN.cern", "LCG.CNAF.it", "LCG.IN2P3.fr", "LCG.GRIDKA.de", "LCG.NCBJ.pl", "AnySite"):
        assert sorted(getDestinationSEList("Tier1-Failover", site=site)) == sorted(resolveSEGroup("Tier1-Failover"))
