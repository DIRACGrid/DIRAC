#! /usr/bin/env python
from DIRAC.Interfaces.API.Transformation        import Transformation
import unittest,types,time

class APITestCase(unittest.TestCase):
  """ TransformationAPI test case """

  def test_setGetReset(self):
    """ Tests the following:
          set*()
          get*()
          setTargetSE()
          setSourceSE()
          getTargetSE()
          getSourceSE()
          reset()
        Ensures that after a reset all parameters are returned to their defaults
    """
        
    transName = 'APITestCaseTransformation-%s' % time.strftime("%Y%m%d-%H:%M:%S")
    
    tAPI = Transformation()
    res = tAPI.getParameters()
    self.assert_(res['OK'])
    defaultParams = res['Value'].copy()
    for parameterName, defaultValue in res['Value'].items():
      if type(defaultValue) in types.StringTypes:
        testValue = "'TestValue'"
      else:
        testValue = '99999'
      execString = "res = tAPI.set%s(%s)" % (parameterName,testValue)
      exec(execString)
      self.assert_(res['OK'])
      execString = "res = tAPI.get%s()" % (parameterName)
      exec(execString)
      self.assert_(res['OK']) 
      self.assertEqual(res['Value'],eval(testValue))

    # Test that SEs delivered as a space or comma seperated string are resolved...
    stringSEs = 'CERN-USER, CNAF-USER GRIDKA-USER,IN2P3-USER'
    listSEs = stringSEs.replace(',',' ').split()
    res = tAPI.setTargetSE(stringSEs)
    self.assert_(res['OK']) 
    res = tAPI.getTargetSE()
    self.assert_(res['OK'])
    self.assertEqual(res['Value'],listSEs)
    # ...but that lists are correctly handled also
    res = tAPI.setSourceSE(listSEs)
    self.assert_(res['OK']) 
    res = tAPI.getSourceSE()
    self.assert_(res['OK'])
    self.assertEqual(res['Value'],listSEs)

    res = tAPI.reset()
    self.assert_(res['OK'])
    res = tAPI.getParameters()
    self.assert_(res['OK'])
    for parameterName, resetValue in res['Value'].items():
      self.assertEqual(resetValue,defaultParams[parameterName])
    self.assertRaises(AttributeError,tAPI.getTargetSE)
    self.assertRaises(AttributeError,tAPI.getSourceSE)

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(APITestCase)  
  testResult = unittest.TextTestRunner(verbosity=3).run(suite)

"""
  # Still to test
  def getTransformation(self,printOutput=False):
  def getTransformationLogging(self,printOutput=False):
  def extendTransformation(self,nTasks, printOutput=False):
  def cleanTransformation(self,printOutput=False):
  def deleteTransformation(self,printOutput=False):
  def addFilesToTransformation(self,lfns, printOutput=False):
  def setFileStatusForTransformation(self,status,lfns,printOutput=False): 
  def getTransformationTaskStats(self,printOutput=False):
  def getTransformationStats(self,printOutput=False):
  def deleteTasks(self,taskMin, taskMax, printOutput=False): 
  def addTaskForTransformation(self,lfns=[],se='Unknown', printOutput=False):
  def setTaskStatus(self, taskID, status, printOutput=False):
  def getTransformationFiles(self,fileStatus=[],lfns=[],outputFields=['FileID','LFN','Status','JobID','TargetSE','UsedSE','ErrorCount','InsertedTime','LastUpdate'], orderBy='FileID', printOutput=False):
  def getTransformationTasks(self,taskStatus=[],taskIDs=[],outputFields=['TransformationID','JobID','WmsStatus','JobWmsID','TargetSE','CreationTime','LastUpdateTime'],orderBy='JobID',printOutput=False):
  def getTransformations(self,transID=[], transStatus=[], outputFields=['TransformationID','Status','AgentType','TransformationName','CreationDate'],orderBy='TransformationID',printOutput=False):
  def addTransformation(self,addFiles=True, printOutput=False):
"""


#tAPI = Transformation(9)

""" Test getting the transformation parameters """
#res = tAPI.getParameters()
#if not res['OK']:
#  print res['Message']
#else:
#  for key,value in res['Value'].items():
#    print key,value

""" Test the ability to get and set individual parameters """
#print tAPI.getAvailable()

#print tAPI.getStatus()
#print tAPI.setStatus('Active')
#print tAPI.getStatus()

""" Test setting imutable parameters """
#print tAPI.getPlugin()
#print tAPI.setPlugin('ByRun')
#print tAPI.getPlugin()

""" Test getting the logging info """
#tAPI.getTransformationLogging(printOutput=True)

""" Test extending transformation """
#tAPI.extendTransformation(100,printOutput=True)

""" Test cleaning/deleting the transformation """
#TODO
#tAPI.cleanTransformation(printOutput=True) 
#tAPI.deleteTransformation(printOutput=True)

""" Test getting all the transformations """
#tAPI.getTransformations(printOutput=True)

""" Test getting the tasks assocated to the transformation """
#tAPI.getTransformationTasks(taskIDs=range(10,100),printOutput=True)

""" Test obtaining the task stats """
#tAPI.getTransformationTaskStats(printOutput=True)
#tAPI.getTransformationStats(printOutput=True)

""" Test obtaining the task files """
#tAPI.getTransformationFiles(orderBy='JobID', printOutput=True)

""" Publish production """
#tAPI = Transformation(9)
#print tAPI.getPlugin()
#tAPI.resetTransformation()
#print tAPI.getPlugin()


#tAPI = Transformation()
#tAPI.setTransformationName("TransformationTestName")
#tAPI.setDescription("This is a short description")
#tAPI.setLongDescription("This is a very long description isnt it")
#tAPI.setType('Replication')
#tAPI.setPlugin('Broadcast')
#tAPI.setTargetSE(['GRIDKA_MC-DST'])
#tAPI.setSourceSE(['CERN_MC_M-DST'])
#print tAPI.addTransformation(printOutput=True)

#tAPI = Transformation()
#print tAPI.generateBKQuery(test=True,printOutput=True)

#res = tAPI.generateBkQuery(test=True,printOutput=True)
#if not res['OK']:
#  print res['Message']
#else:
#  bkQuery = res['Value']
#  print bkQuery
#  print tAPI.setBkQuery(bkQuery)
#  print tAPI.getBkQuery()
#  print tAPI.getBkQueryID()
#  print tAPI.removeTransformationBkQuery()
#  print tAPI.cleanTransformation(printOutput=True)
