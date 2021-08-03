""" This integration test runs the RMS test scenari using
the MySQL DB
"""

# pylint: disable=invalid-name,wrong-import-position

from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()


from pytest import fixture

from DIRAC.RequestManagementSystem.DB.RequestDB import RequestDB
from DIRAC.RequestManagementSystem.DB.test.RMSTestScenari import test_dirty,\
    test_scheduled,\
    test_stress,\
    test_stressBulk


@fixture(scope="function")
def reqDB(request):
  """ This fixture just instanciate the RequestDB
  """
  yield RequestDB()
