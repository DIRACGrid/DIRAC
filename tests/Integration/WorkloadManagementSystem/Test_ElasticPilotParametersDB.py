""" This tests only need the ElasticPilotParametersDB, and connects directly to it
"""

import time

import DIRAC

DIRAC.initialize()  # Initialize configuration

from DIRAC import gLogger
from DIRAC.WorkloadManagementSystem.DB.ElasticPilotParametersDB import ElasticPilotParametersDB

#  Add a time delay to allow updating the modified index before querying it.
SLEEP_DELAY = 2

gLogger.setLevel("DEBUG")
elasticPilotParametersDB = ElasticPilotParametersDB()


def test_setAndGetPilotFromDB():
    res = elasticPilotParametersDB.setPilotParameter(100, "DIRAC", "dirac@cern")
    assert res["OK"]
    time.sleep(SLEEP_DELAY)

    res = elasticPilotParametersDB.getPilotParameters(100)
    assert res["OK"]
    assert res["Value"][100]["DIRAC"] == "dirac@cern"

    # update it
    res = elasticPilotParametersDB.setPilotParameter(100, "DIRAC", "dirac@cern.cern")
    assert res["OK"]
    time.sleep(SLEEP_DELAY)
    res = elasticPilotParametersDB.getPilotParameters(100)
    assert res["OK"]
    assert res["Value"][100]["DIRAC"] == "dirac@cern.cern"
    res = elasticPilotParametersDB.getPilotParameters(100, ["DIRAC"])
    assert res["OK"]
    assert res["Value"][100]["DIRAC"] == "dirac@cern.cern"
    res = elasticPilotParametersDB.getPilotParameters(100, "DIRAC")
    assert res["OK"]
    assert res["Value"][100]["DIRAC"] == "dirac@cern.cern"

    # add one
    res = elasticPilotParametersDB.setPilotParameter(100, "someKey", "someValue")
    assert res["OK"]
    time.sleep(SLEEP_DELAY)

    # now search
    res = elasticPilotParametersDB.getPilotParameters(100)
    assert res["OK"]
    assert res["Value"][100]["DIRAC"] == "dirac@cern.cern"
    assert res["Value"][100]["someKey"] == "someValue"
    res = elasticPilotParametersDB.getPilotParameters(100, ["DIRAC", "someKey"])
    assert res["OK"]
    assert res["Value"][100]["DIRAC"] == "dirac@cern.cern"
    assert res["Value"][100]["someKey"] == "someValue"
    res = elasticPilotParametersDB.getPilotParameters(100, "DIRAC, someKey")
    assert res["OK"]
    assert res["Value"][100]["DIRAC"] == "dirac@cern.cern"
    assert res["Value"][100]["someKey"] == "someValue"

    # another one + search
    res = elasticPilotParametersDB.setPilotParameter(100, "someOtherKey", "someOtherValue")
    assert res["OK"]
    time.sleep(SLEEP_DELAY)
    res = elasticPilotParametersDB.getPilotParameters(100)
    assert res["OK"]
    assert res["Value"][100]["DIRAC"] == "dirac@cern.cern"
    assert res["Value"][100]["someKey"] == "someValue"
    assert res["Value"][100]["someOtherKey"] == "someOtherValue"
    res = elasticPilotParametersDB.getPilotParameters(100, ["DIRAC", "someKey", "someOtherKey"])
    assert res["OK"]
    assert res["Value"][100]["DIRAC"] == "dirac@cern.cern"
    assert res["Value"][100]["someKey"] == "someValue"
    assert res["Value"][100]["someOtherKey"] == "someOtherValue"

    # another pilot
    res = elasticPilotParametersDB.setPilotParameter(101, "DIRAC", "dirac@cern")
    assert res["OK"]
    res = elasticPilotParametersDB.setPilotParameter(101, "key101", "value101")
    assert res["OK"]
    res = elasticPilotParametersDB.setPilotParameter(101, "someKey", "value101")
    assert res["OK"]
    res = elasticPilotParametersDB.setPilotParameter(101, "key101", "someValue")
    assert res["OK"]
    time.sleep(SLEEP_DELAY)
    res = elasticPilotParametersDB.getPilotParameters(100)
    assert res["OK"]
    assert res["Value"][100]["DIRAC"] == "dirac@cern.cern"
    assert res["Value"][100]["someKey"] == "someValue"
    assert res["Value"][100]["someOtherKey"] == "someOtherValue"
    assert len(res["Value"]) == 1
    assert len(res["Value"][100]) == 5  # Added two extra because of the new timestamp and ID field in the mapping
    res = elasticPilotParametersDB.getPilotParameters(101)
    assert res["OK"]
    assert res["Value"][101]["DIRAC"] == "dirac@cern"
    assert res["Value"][101]["key101"] == "someValue"
    assert res["Value"][101]["someKey"] == "value101"
    assert len(res["Value"]) == 1
    assert len(res["Value"][101]) == 5  # Same thing as with doc 100
    res = elasticPilotParametersDB.setPilotParameters(101, [("k", "v"), ("k1", "v1"), ("k2", "v2")])
    assert res["OK"]
    time.sleep(SLEEP_DELAY)
    res = elasticPilotParametersDB.getPilotParameters(101)
    assert res["OK"]
    assert res["Value"][101]["DIRAC"] == "dirac@cern"
    assert res["Value"][101]["k"] == "v"
    assert res["Value"][101]["k2"] == "v2"

    # another pilot with pilotID > 1000000
    res = elasticPilotParametersDB.setPilotParameters(1010000, [("k", "v"), ("k1", "v1"), ("k2", "v2")])
    assert res["OK"]
    time.sleep(SLEEP_DELAY)
    res = elasticPilotParametersDB.getPilotParameters(1010000)
    assert res["Value"][1010000]["k"] == "v"
    assert res["Value"][1010000]["k2"] == "v2"

    # deleting
    res = elasticPilotParametersDB.deletePilotParameters(100)
    assert res["OK"]
    time.sleep(SLEEP_DELAY)
    res = elasticPilotParametersDB.getPilotParameters(100)
    assert res["OK"]
    assert len(res["Value"][100]) == 0

    res = elasticPilotParametersDB.deletePilotParameters(101, "someKey")
    assert res["OK"]
    time.sleep(SLEEP_DELAY)
    res = elasticPilotParametersDB.getPilotParameters(101)
    assert res["OK"]
    assert len(res["Value"][101]) == 7
    res = elasticPilotParametersDB.deletePilotParameters(101, "someKey, key101")  # someKey is already deleted
    assert res["OK"]
    time.sleep(SLEEP_DELAY)
    res = elasticPilotParametersDB.getPilotParameters(101)
    assert res["OK"]
    assert len(res["Value"][101]) == 6
    res = elasticPilotParametersDB.deletePilotParameters(101, "nonExistingKey")
    assert res["OK"]
    time.sleep(SLEEP_DELAY)
    res = elasticPilotParametersDB.getPilotParameters(101)
    assert res["OK"]
    assert len(res["Value"][101]) == 6

    res = elasticPilotParametersDB.deletePilotParameters(1010000)
    assert res["OK"]
    time.sleep(SLEEP_DELAY)
    res = elasticPilotParametersDB.getPilotParameters(1010000)
    assert res["OK"]
    assert len(res["Value"][1010000]) == 0

    # delete the indexes
    res = elasticPilotParametersDB.deleteIndex(elasticPilotParametersDB.indexName_base)
    assert res["OK"]
    assert res["Value"] == "Nothing to delete"
    res = elasticPilotParametersDB.deleteIndex(elasticPilotParametersDB._indexName(100))
    assert res["OK"]
    res = elasticPilotParametersDB.deleteIndex(elasticPilotParametersDB._indexName(1010000))
    assert res["OK"]
