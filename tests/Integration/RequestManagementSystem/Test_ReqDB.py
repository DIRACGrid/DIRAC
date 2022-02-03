""" This integration test runs the RMS test scenari using
the MySQL DB
"""

# pylint: disable=invalid-name,wrong-import-position
import DIRAC

DIRAC.initialize()  # Initialize configuration

from pytest import fixture

from DIRAC.RequestManagementSystem.DB.RequestDB import RequestDB
from DIRAC.RequestManagementSystem.DB.test.RMSTestScenari import (
    test_dirty,
    test_scheduled,
    test_stress,
    test_stressBulk,
)


@fixture(scope="function")
def reqDB(request):
    """This fixture just instanciate the RequestDB"""
    yield RequestDB()
