"""
OBSOLETE
K.C.

"""

import unittest, types, time
from DIRAC.DataManagementSystem.DB.TransferDB import TransferDB
from DIRAC.RequestManagementSystem.DB.RequestDB import RequestDB
from DIRAC.RequestManagementSystem.Client.DataManagementRequest import DataManagementRequest


class FullChainTestCase(unittest.TestCase):
  """ Base class for the TransferDB test cases
  """
  def setUp(self):
    self.TransferDB = TransferDB()
    self.RequestDB = RequestDB('mysql')

class CreateInsertScheduleSubmitRemoveCase(FullChainTestCase):

  def test_fullChain(self):


    ##########################################################
    # This is the section to be done by the data manager or whoever

    # Create the DataManagementRequest
    dmRequest = DataManagementRequest()
    requestID = 9999999
    dmRequest.setRequestID(requestID)
    requestType = 'transfer'
    res = dmRequest.initiateSubRequest(requestType)
    ind = res['Value']
    subRequestDict = {'Operation':'Replicate','SourceSE':'CERN-disk','TargetSE':'SARA-SRM2','Catalogue':'LFC','Status':'Waiting','SubRequestID':0,'SpaceToken':'LHCb_RAW'}
    res = dmRequest.setSubRequestAttributes(ind,requestType,subRequestDict)
    lfn = '/lhcb/production/DC06/phys-v2-lumi5/00001925/DST/0000/00001925_00000090_5.dst'
    files = []
    fileDict = {'FileID':9999,'LFN':lfn,'Size':10000,'PFN':'','GUID':'','Md5':'','Addler':'','Attempt':0,'Status':'Waiting'}
    files.append(fileDict)
    res = dmRequest.setSubRequestFiles(ind,requestType,files)
    requestName = 'test-Request-%s' % time.time()
    dmRequest.setRequestName(requestName)
    jobID = 0
    dmRequest.setJobID(jobID)
    ownerDN = '/C=Country/O=Organisation/OU=Unit/L=Location/CN=Name'
    dmRequest.setOwnerDN(ownerDN)
    diracInstance = 'developement'
    dmRequest.setDiracInstance(diracInstance)
    res = dmRequest.toXML()
    requestString = res['Value']
    print requestString
    # Insert the request in the request DB
    res = self.RequestDB.setRequest(requestType,requestName,'Dummy',requestString)
    print res,'RequestDB.setRequest()'
    self.assert_(res['OK'])
    self.assertEqual(type(res['Value']),types.IntType)
    requestID = res['Value']

    """
    ##########################################################
    # This is the section to be done by the replication scheduler

    # Get the request back
    res = self.RequestDB.getRequest(requestType)
    print res, 'RequestDB.getRequest()'
    self.assert_(res['OK'])
    dmRequest = DataManagementRequest(res['Value']['RequestString'])
    res = dmRequest.getNumSubRequests('transfer')
    print res,'dmRequest.getNumSubRequests()'
    for ind in range(res['Value']):
      res = dmRequest.getSubRequestAttributes(ind, 'transfer')
      print res,'dmRequest.getSubRequestAttributes()'
      attributes = res['Value']
      sourceSE = attributes['SourceSE']
      targetSE = attributes['TargetSE']
      res = dmRequest.getSubRequestFiles(ind,'transfer')
      print res,'dmRequest.getSubRequestFiles()'
      for file in res['Value']:
        fileID = file['FileID']
        lfn = file['LFN']
        print fileID,lfn
    sourceSURL = 'srm://sourceHost/sourceSAPath/%s' % lfn
    targetSURL = 'srm://targetHost/targetSAPath/%s' % lfn

    # Determine the channel
    res = self.TransferDB.getChannelID(sourceSE,targetSE)
    print res,'TransferDB.getChannelID()'
    if not res['OK']:
      res = self.TransferDB.createChannel(sourceSE, targetSE)
      print res,'TransferDB.createChannel()'
      channelID = res['Value']['ChannelID']
    else:
      channelID = res['Value']
    # Assign the files to the channel
    res = self.TransferDB.addFileToChannel(channelID, fileID, sourceSURL, targetSURL)
    print res,'TransferDB.addFileToChannel()'

    ##########################################################
    # This is the section to be done by the FTS Agent

    ##############This section is the submission of the requests
    # Need a method here to select which channel to submit to based on the number of jobs running there.
    # Then get the files from the channel
    res = self.TransferDB.getFilesForChannel(channelID,50)
    print res,'TransferDB.getFilesForChannel()'
    # Create a dummy FTSReq entry and the FileToFTS entries
    ftsGUID = 'THIS IS A MADE UP GUID'
    ftsServer = 'http://ftsServer:port/webservice/url'
    res = self.TransferDB.insertFTSReq(ftsGUID,ftsServer,channelID)
    print res,'TransferDB.insertFTSReq()'
    ftsReqID = res['Value']
    #Update the FTSReqFiles table so we know which files were being transfered
    res = self.TransferDB.setFTSReqFiles(ftsReqID, [fileID])
    print res,'TransferDB.setFTSReqFiles()'
    # Since the files are now submitted they can be removed from the channel
    res = self.TransferDB.removeFileFromChannel(channelID, fileID)
    print res,'TransferDB.removeFileFromChannel()'

    #############This section is the monitoring of the requests
    # Get the FTS req details to be monitored
    res = self.TransferDB.getFTSReq()
    print res,'TransferDB.getFTSReq()'
    ftsReqID = res['Value']['FTSReqID']
    ftsGUID = res['Value']['FTSGuid']
    ftsServer = res['Value']['FTSServer']
    # Get the LFNS associated to the request
    res = self.TransferDB.getFTSReqLFNs(ftsReqID)
    print res,'TransferDB.getFTSReqLFNs()'
    files = res['Value']

    #Finish the FTS request and update the status accordingly
    # Check that we can update the status of the file
    res = self.TransferDB.setFileToFTSFileAttribute(ftsReqID,fileID,'Duration',10)
    print res,'TransferDB.setFileToFTSFileAttribute()'
    res = self.TransferDB.setFileToFTSFileAttribute(ftsReqID,fileID,'Status','Failed')
    print res,'TransferDB.setFileToFTSFileAttribute()'
    res = self.TransferDB.setFileToFTSFileAttribute(ftsReqID,fileID,'Reason','Because the SRMs are rubbish.')
    print res,'TransferDB.setFileToFTSFileAttribute()'
    # Now set the FTSReq status to terminal so that it is not monitored again
    res = self.TransferDB.setFTSReqStatus(ftsReqID,'Finished')
    print res,'TransferDB.setFTSReqStatus()'

    # Need something to work out the last throughput business
    # This should be finished.
    """

if __name__ == '__main__':

  suite = unittest.defaultTestLoader.loadTestsFromTestCase(CreateInsertScheduleSubmitRemoveCase)
  #suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(CreateFTSReqCase))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)

