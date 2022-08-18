""" This runs the RMS scenari as unit test for the DB. For that,
it replaces the normal MySQL connection with an inmemory SQLite db
"""

# pylint: disable=invalid-name,wrong-import-position
from unittest.mock import patch
from pytest import fixture

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from DIRAC import gLogger

from DIRAC.RequestManagementSystem.DB import RequestDB

from DIRAC.RequestManagementSystem.DB.test.RMSTestScenari import (
    test_dirty,
    test_scheduled,
    test_stress,
    test_stressBulk,
)


@fixture(scope="function")
def reqDB(request):
    """This fixture instanciate a RequestDB with an in memory sqlite backend"""

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
