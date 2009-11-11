### UnitTest class for DB

import unittest
from DIRAC.ResourceStatusSystem.test.mock import Mock
from DIRAC.ResourceStatusSystem.DB.ResourceStatusDB import ResourceStatusDB

class ResourceStatusDBTestCase(unittest.TestCase):
  """ Base class for the ResourceStatusDB test cases
  """
  def setUp(self):

    canned_Sites_rows = [
    [u'CNAF', u'T1', u'descCNAF', u'2009-07-09 10:00:17', '2009-07-09 10:00:17', '9999-12-31 23:59:59', 'Federico'],
    [u'Ferrara', u'T2', u'descFerrara', u'2009-07-09 10:00:17', '2009-07-09 10:00:17', '9999-12-31 23:59:59', 'Federico'],
    ]

    canned_SitesHistory_rows = [
    [u'oldCNAF', u'T1', u'descCNAF', u'2009-07-09 10:00:17', '2009-07-09 10:00:17', '9999-12-31 23:59:59', 'Federico'],
    [u'oldFerrara', u'T2', u'descFerrara', u'2009-07-09 10:00:17', '2009-07-09 10:00:17', '9999-12-31 23:59:59', 'Federico'],
    ]

    canned_SiteTypes_rows = [
    [u'T1', u'descT1'],
    [u'T2', u'descT2'],
    ]

    canned_ResourceTypes_rows = [
    [u'CE', u'descCE'],
    [u'SE', u'descSE'],
    ]

    canned_ResourceStatus_rows = [
    [u'CE01', u'CE', u'CNAF', u'Active', u'ok', u'2009-07-09 10:00:17', '2009-07-09 10:00:17', '9999-12-31 23:59:59', 'Federico'],
    [u'CE0001', u'CE', u'Ferrara', u'Active', u'ok', u'2009-07-09 10:00:17', '2009-07-09 10:00:17', '9999-12-31 23:59:59', 'Federico'],
    ]

    canned_ResourceStatusHistory_rows = [
    [u'CE01', u'CE', u'CNAF', u'Active', u'ok', u'2009-07-09 10:00:19', '2009-07-09 10:00:17', '9999-12-31 23:59:59', 'Federico'],
    [u'CE0001', u'CE', u'Ferrara', u'Active', u'ok', u'2009-07-09 10:00:19', '2009-07-09 10:00:17', '9999-12-31 23:59:59', 'Federico'],
    ]

    # Create a mock
    mock_rsDB = Mock()

    mock_rsDB._query.return_value = {'OK': ''}

    # setting mock interface
    mock_rsDB.getSites.return_value = canned_Sites_rows
    mock_rsDB.getSitesHistory.return_value = canned_SitesHistory_rows
    mock_rsDB.getSiteTypes.return_value = canned_SiteTypes_rows
    mock_rsDB.getResourceTypes.return_value = canned_ResourceTypes_rows
    mock_rsDB.getResourceStatus.return_value = canned_ResourceStatus_rows
    mock_rsDB.getResourceStatusHistory.return_value = canned_ResourceStatusHistory_rows

    # Run the test
    self.myDB = ResourceStatusDB(mock_rsDB)



class ResourceStatusDBSuccess(ResourceStatusDBTestCase):

  def test_getSites(self):
    res = self.myDB.getSites('CNAF')
    # Verify that mocks were used as expected
    #mock_snaplogic_manager.get_attrib_values.assert_called_with(self.appname, self.hostname)

    self.assertEqual(res, [u'CNAF', u'T1', u'descCNAF', u'2009-07-09 10:00:17', '2009-07-09 10:00:17', '9999-12-31 23:59:59', 'Federico'],)

#  def test_AddSiteTypes(self):



#class SitesTest(ResourceStatusDBTestCase):
#  def test_addSite(self):
#    pass
#
#
#class SiteTypesTest(ResourceStatusDBTestCase):
#
#  def test_AddSiteTypes(self):
#    type = 'Tx'
#    description = 'test'
#    self.rsDB.addSiteType(type, description)
#    result = rsDB.getSiteType('Tx')
#    self.assertEqual(description, result['Description'])
#
#  def test_RemoveSiteTypes(self):
#    type = 'Tx'
#    description = 'test'
#    self.rsDB.addSiteType(type, description)
#    self.rsDB.removeSiteType(type)
#    result = rsDB.getSiteType('Tx')
#    self.assert_()
#
#
#
#class ResourceTypesTest(ResourceStatusDBTestCase):
#  pass
#
#class ResourceStatusTest(ResourceStatusDBTestCase):
#  pass
#
#
#
#
#
#
#
## da capire che significa:
#
if __name__ == '__main__':
  unittest.main()

#
#  suite = unittest.defaultTestLoader.loadTestsFromTestCase(SitesTest)
#  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(SiteTypesTest))
#  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(ResourceTypesTest))
#  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(ResourceStatusTest))
#
#  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
