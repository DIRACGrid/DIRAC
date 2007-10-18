import unittest,types,time
from DIRAC.DataManagementSystem.DB.TransferDB import TransferDB

class TransferDBTestCase(unittest.TestCase):
  """ Base class for the TransferDB test cases
  """
  def setUp(self):
    self.TransferDB = TransferDB()

class CreateChannelCase(TransferDBTestCase):

  def test_createChannel(self):

    result = self.TransferDB.createChannel('Source','Destination')
    print result
    self.assert_(result['OK'])

  def test_getChannels(self):
    result = self.TransferDB.getChannels()
    print result
    self.assert_(result['OK'])

  def test_getChannelAttribute(self):
    res = self.TransferDB.getChannelID('Source','Destination')
    self.assert_(res['OK'])
    channelID = res['Value']
    res = self.TransferDB.getChannelAttribute(channelID,'SourceSite')
    print res
    self.assertEqual(res['Value'],'Source')

  def test_setChannelAttribute(self):
    res = self.TransferDB.getChannelID('Source','Destination')
    self.assert_(res['OK'])
    channelID = res['Value']
    testValue = 100.5
    res = self.TransferDB.setChannelAttribute(channelID,'LatestThroughPut',testValue)
    self.assert_(res['OK'])
    res = self.TransferDB.getChannelAttribute(channelID,'LatestThroughPut')
    self.assertEqual(res['Value'],testValue)

  def test_addFileToChannel(self):
    res = self.TransferDB.getChannelID('Source','Destination')
    self.assert_(res['OK'])
    channelID = res['Value']
    fileID = 999999
    sourceSURL = 'srm:/sourceHost:port/base/fileName'
    targetSURL = 'srm:/targetHost:port/base/fileName'
    res = self.TransferDB.addFileToChannel(channelID,fileID,sourceSURL,targetSURL)
    if not res['OK']:
      res = self.TransferDB.removeFileFromChannel(channelID,fileID)
      self.assert_(res['OK'])
      res = self.TransferDB.addFileToChannel(channelID,fileID,sourceSURL,targetSURL)
    self.assert_(res['OK'])
    status = 'Submitted'
    res = self.TransferDB.setFileChannelStatus(channelID,fileID,status)
    self.assert_(res['OK'])


if __name__ == '__main__':

  suite = unittest.defaultTestLoader.loadTestsFromTestCase(CreateChannelCase)
  #suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(RequestRemovalCase))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)

