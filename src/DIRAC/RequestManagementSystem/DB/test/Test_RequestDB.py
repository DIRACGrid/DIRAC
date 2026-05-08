""" This runs the RMS scenari as unit test for the DB. For that,
it replaces the normal MySQL connection with an inmemory SQLite db
"""

# pylint: disable=invalid-name,wrong-import-position
from unittest.mock import patch
from pytest import fixture

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from DIRAC import gLogger, S_OK

from DIRAC.RequestManagementSystem.DB import RequestDB
from DIRAC.RequestManagementSystem.Client.File import File
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.Client.Request import Request

from DIRAC.RequestManagementSystem.DB.test.RMSTestScenari import (  # pylint: disable=unused-import
    test_dirty,
    test_scheduled,
    test_stress,
    test_stressBulk,
)


@fixture(scope="function")
def reqDB(mocker, request):
    """This fixture instanciate a RequestDB with an in memory sqlite backend"""
    mocker.patch("DIRAC.RequestManagementSystem.DB.RequestDB.getDNForUsername", return_value=S_OK(["/bih/boh/DN"]))

    def mock_requestDB__init__(self):
        """This mock creates the RequestDB with an in memory sqlite backend"""
        self.log = gLogger.getSubLogger("RequestDB")
        # Initialize the connection info
        self.engine = create_engine("sqlite:///:memory:", echo=False, pool_recycle=3600)
        RequestDB.metadata.bind = self.engine
        self.DBSession = sessionmaker(bind=self.engine)

    with patch.object(RequestDB.RequestDB, "__init__", mock_requestDB__init__):
        db = RequestDB.RequestDB()
        db.createTables()

        yield db


def test_web_queries_reject_unknown_attributes(reqDB):
    request = Request({"RequestName": "web-summary"})
    operation = Operation({"Type": "RemoveReplica", "TargetSE": "CERN-USER"})
    operation += File({"LFN": "/lhcb/user/c/cibak/web-summary"})
    request += operation

    put = reqDB.putRequest(request)
    assert put["OK"], put

    summary = reqDB.getRequestSummaryWeb({"Type": "RemoveReplica"}, [("RequestID", "ASC")], 0, 10)
    assert summary["OK"], summary
    assert summary["Value"]["TotalRecords"] == 1, summary

    counters = reqDB.getRequestCountersWeb("Type", {"Status": "Waiting"})
    assert counters["OK"], counters
    assert counters["Value"] == {"RemoveReplica": 1}, counters

    distinct = reqDB.getDistinctValues("Operation", "Type")
    assert distinct["OK"], distinct
    assert distinct["Value"] == ["RemoveReplica"], distinct

    invalid_summary = reqDB.getRequestSummaryWeb({"__class__": "Request"}, [], 0, 10)
    assert not invalid_summary["OK"], invalid_summary
    assert invalid_summary["Message"] == "Unknown Request attribute '__class__'"

    invalid_counters = reqDB.getRequestCountersWeb("__class__", {})
    assert not invalid_counters["OK"], invalid_counters
    assert invalid_counters["Message"] == "Unknown Request attribute '__class__'"

    invalid_distinct = reqDB.getDistinctValues("Request", "__class__")
    assert not invalid_distinct["OK"], invalid_distinct
    assert invalid_distinct["Message"] == "Unknown Request attribute '__class__'"
