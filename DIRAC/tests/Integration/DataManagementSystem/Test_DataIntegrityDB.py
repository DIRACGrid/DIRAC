""" This is a test of the IntegrityDB

    It supposes that the DB is present.

    This is pytest!
"""
# pylint: disable=invalid-name,wrong-import-position
import time

from DIRAC import gLogger

import DIRAC

DIRAC.initialize()  # Initialize configuration

from DIRAC.DataManagementSystem.DB.DataIntegrityDB import DataIntegrityDB


def test_DataIntegrityDB():
    """Some test cases"""
    source = "Test"
    prognosis = "TestError"
    prodID = 1234
    timestamp = int(time.time())
    lfn = "/Test/%08d/File1/%d" % (prodID, timestamp)
    pfn = "File1/%d" % (timestamp)
    fileMetadata1 = {lfn: {"Prognosis": prognosis, "PFN": pfn, "SE": "Test-SE"}}
    fileOut1 = {"LFN": lfn, "PFN": pfn, "Prognosis": prognosis, "GUID": None, "SE": "Test-SE", "Size": None}
    newStatus = "Solved"
    newPrognosis = "AnotherError"

    diDB = DataIntegrityDB()

    # Clean up the database if required
    result = diDB.getTransformationProblematics(1234)
    assert result["OK"], result["Message"]
    for fileID in result["Value"].values():
        result = diDB.removeProblematic(fileID)
        assert result["OK"], result["Message"]
    result = diDB.getProblematicsSummary()
    assert result["OK"], result["Message"]
    assert result["Value"] == {}

    # Run the actual test
    result = diDB.insertProblematic(source, fileMetadata1)
    assert result["OK"], result["Message"]
    assert result["Value"] == {"Successful": {lfn: True}, "Failed": {}}

    result = diDB.insertProblematic(source, fileMetadata1)
    assert result["OK"], result["Message"]
    assert result["Value"] == {"Successful": {lfn: "Already exists"}, "Failed": {}}

    result = diDB.getProblematicsSummary()
    assert result["OK"], result["Message"]
    assert result["Value"] == {"TestError": {"New": 1}}

    result = diDB.getDistinctPrognosis()
    assert result["OK"], result["Message"]
    assert result["Value"] == ["TestError"]

    result = diDB.getProblematic()
    assert result["OK"], result["Message"]
    fileOut1["FileID"] = result["Value"]["FileID"]
    assert result["Value"] == fileOut1

    result = diDB.incrementProblematicRetry(result["Value"]["FileID"])
    assert result["OK"], result["Message"]
    assert result["Value"] == 1

    result = diDB.getProblematic()
    assert result["OK"], result["Message"]
    assert result["Value"] == fileOut1

    result = diDB.getPrognosisProblematics(prognosis)
    assert result["OK"], result["Message"]
    assert result["Value"] == [fileOut1]

    result = diDB.getTransformationProblematics(prodID)
    assert result["OK"], result["Message"]
    assert result["Value"][lfn] == fileOut1["FileID"]

    result = diDB.setProblematicStatus(fileOut1["FileID"], newStatus)
    assert result["OK"], result["Message"]
    assert result["Value"] == 1

    result = diDB.changeProblematicPrognosis(fileOut1["FileID"], newPrognosis)
    assert result["OK"], result["Message"]
    assert result["Value"] == 1

    result = diDB.getPrognosisProblematics(prognosis)
    assert result["OK"], result["Message"]
    assert result["Value"] == []

    result = diDB.removeProblematic(fileOut1["FileID"])
    assert result["OK"], result["Message"]
    assert result["Value"] == 1

    result = diDB.getProblematicsSummary()
    assert result["OK"], result["Message"]
    assert result["Value"] == {}

    gLogger.info("\n OK\n")
