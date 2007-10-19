import unittest,types,time
from DIRAC.DataManagementSystem.DB.TransferDB import TransferDB

class TransferDBTestCase(unittest.TestCase):
  """ Base class for the TransferDB test cases
  """
  def setUp(self):
    self.TransferDB = TransferDB()

class CreateChannelCase(TransferDBTestCase):

  def test_createChannel(self):

    # Create a channel to be used in the rest of the test
    res = self.TransferDB.createChannel('Source','Destination')
    print res,'createChannel()'
    self.assert_(res['OK'])

    # Check that we can get the channels back
    res = self.TransferDB.getChannels()
    print res,'getChannels()'
    self.assert_(res['OK'])

    # Check that we can get the channel ID
    res = self.TransferDB.getChannelID('Source','Destination')
    print res,'getChannelID()'
    self.assert_(res['OK'])
    channelID = res['Value']

    # Check that we can retrieve attributes about the channel
    res = self.TransferDB.getChannelAttribute(channelID,'SourceSite')
    print res,'getChannelAttribute()'
    self.assertEqual(res['Value'],'Source')

    # Check that we are able to set channel attribute
    testValue = 100.5
    res = self.TransferDB.setChannelAttribute(channelID,'LatestThroughPut',testValue)
    print res,'setChannelAttribute()'
    self.assert_(res['OK'])

    # Check that is was set correctly
    res = self.TransferDB.getChannelAttribute(channelID,'LatestThroughPut')
    print res,'getChannelAttribute()'
    self.assertEqual(res['Value'],testValue)

    # Now check that we can add a file to the channel we have created
    fileID = 999999
    sourceSURL = 'srm:/sourceHost:port/base/fileName'
    targetSURL = 'srm:/targetHost:port/base/fileName'
    res = self.TransferDB.addFileToChannel(channelID,fileID,sourceSURL,targetSURL)
    print res,'addFileToChannel()'
    self.assert_(res['OK'])

    # Now check that we are able to change the files status
    status = 'Made up status'
    res = self.TransferDB.setFileChannelStatus(channelID,fileID,status)
    print res,'setFileChannelStatus()'
    self.assert_(res['OK'])

    # Check that it was correctly set
    res = self.TransferDB.getFileChannelAttribute(channelID,fileID,'Status')
    print res,'getFileChannelAttribute()'
    self.assertEqual(res['Value'],status)

    # Now remove the file to clean up
    res = self.TransferDB.removeFileFromChannel(channelID,fileID)
    print res,'removeFileFromChannel()'
    self.assert_(res['OK'])

class CreateFTSReqCase(TransferDBTestCase):

  def test_insertFTSReq(self):
    #Check that things don't break if there are not requests
    res = self.TransferDB.getFTSReq()
    print res,'getFTSReq()'
    self.assert_(res['OK'])
    self.assertFalse(res['Value'])

    #Check that we can write an FTS request to the DB and get the FTSReqID back
    res = self.TransferDB.getChannelID('Source','Destination')
    print res,'getChannelID()'
    self.assert_(res['OK'])
    channelID = res['Value']
    ftsGUID = 'FAKE-FTS-GUID'
    ftsServer = 'https://ftsFake.cern.ch:port/path/to/web/service'
    res = self.TransferDB.insertFTSReq(ftsGUID,ftsServer,channelID)
    print res,'insertFTSReq()'
    self.assert_(res['OK'])
    self.assertEqual(type(res['Value']),types.LongType)
    ftsReqID = res['Value']

    #Check that we can get the request back
    res = self.TransferDB.getFTSReq()
    print res,'getFTSReq()'
    self.assert_(res['OK'])
    self.assertTrue(res['Value'])
    self.assertEqual(res['Value']['FTSGuid'],ftsGUID)
    self.assertEqual(res['Value']['FTSServer'],ftsServer)
    self.assertEqual(res['Value']['FTSReqID'],ftsReqID)

    #Check that we can change the status of the request.
    status = 'Non-existant status'
    res = self.TransferDB.setFTSReqStatus(ftsReqID,status)
    print res,'setFTSReqStatus()'
    self.assert_(res['OK'])

    # Check now that we can't get the request
    res = self.TransferDB.getFTSReq()
    print res,'getFTSReq()'
    self.assert_(res['OK'])
    self.assertFalse(res['Value'])

    # Check that we can delete the request
    res = self.TransferDB.deleteFTSReq(ftsReqID)
    print res,'deleteFTSReq()'
    self.assert_(res['OK'])

class CreateFileToFTSCase(TransferDBTestCase):

  def test_setFileToFTS(self):
    # Insert a FTSReq such that we get a FTSReqID back
    res = self.TransferDB.getChannelID('Source','Destination')
    print res,'getChannelID()'
    self.assert_(res['OK'])
    channelID = res['Value']
    ftsGUID = 'FAKE-FTS-GUID'
    ftsServer = 'https://ftsFake.cern.ch:port/path/to/web/service'
    res = self.TransferDB.insertFTSReq(ftsGUID,ftsServer,channelID)
    print res,'insertFTSReq()'
    self.assert_(res['OK'])
    self.assertEqual(type(res['Value']),types.LongType)
    ftsReqID = res['Value']

    # Now associate some files with the FTSReqID
    fileIDs = [1,2,3,4,5,6,7,8]
    res = self.TransferDB.setFTSReqFiles(ftsReqID,fileIDs)
    print res,'setFTSReqFiles()'
    self.assert_(res['OK'])
    res = self.TransferDB.getFTSReqFileIDs(ftsReqID)
    print res, 'getFTSReqFileIDs()'
    self.assert_(res['OK'])
    self.assertEqual(fileIDs,res['Value'])

    # Now remove the entries in the FileToFTS table
    res = self.TransferDB.removeFilesFromFTSReq(ftsReqID)
    print res,'removeFilesFromFTSReq()'
    self.assert_(res['OK'])
    res = self.TransferDB.getFTSReqFileIDs(ftsReqID)
    print res, 'getFTSReqFileIDs()'
    self.assertFalse(res['OK'])
    # Now delete the FTS request
    res = self.TransferDB.deleteFTSReq(ftsReqID)
    print res,'deleteFTSReq()'
    self.assert_(res['OK'])


if __name__ == '__main__':

  suite = unittest.defaultTestLoader.loadTestsFromTestCase(CreateChannelCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(CreateFTSReqCase))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(CreateFileToFTSCase))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)

