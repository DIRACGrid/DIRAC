""" tests for the JobDB module """

# pylint: disable=protected-access, missing-docstring

from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

import unittest

from mock import MagicMock, patch

from DIRAC import S_OK

MODULE_NAME = "DIRAC.WorkloadManagementSystem.DB.JobDB"


class JobDBTest(unittest.TestCase):

  def setUp(self):

    def mockInit(self):
      self.log = MagicMock()
      self.logger = MagicMock()
      self._connected = True

    from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
    with patch(MODULE_NAME + ".JobDB.__init__", new=mockInit):
      self.jobDB = JobDB()
    self.jobDB._query = MagicMock(name="Query")
    self.jobDB._escapeString = MagicMock(return_value=S_OK())

  def tearDown(self):
    pass

  def test_getInputData(self):
    self.jobDB._query.return_value = S_OK((('/vo/user/lfn1',), ('LFN:/vo/user/lfn2',)))
    result = self.jobDB.getInputData(1234)
    print(result)
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'], ['/vo/user/lfn1', '/vo/user/lfn2'])
