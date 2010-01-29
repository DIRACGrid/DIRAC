#! /usr/bin/env python
from DIRAC.Interfaces.API.Transformation        import Transformation
from DIRAC.Core.Utilities.List                  import sortList
import unittest,types,time

class APITestCase(unittest.TestCase):

  def setUp(self):
    self.transID = 0
    transName = 'APITestCaseTransformation-%s' % time.strftime("%Y%m%d-%H:%M:%S")
    res = self.__createTransformation(transName)
    self.assert_(res['OK'])
    self.transID = res['Value']

  def tearDown(self):
    if self.transID:
      tAPI = Transformation(self.transID)
      tAPI.deleteTransformation()

  def __createTransformation(self,transName):
    tAPI = Transformation()
    res = tAPI.setTransformationName(transName)
    self.assert_(res['OK'])
    description = 'Test transforamtion description'
    res = tAPI.setDescription(description)
    longDescription = 'Test transforamtion long description'
    res = tAPI.setLongDescription(longDescription)
    self.assert_(res['OK'])
    res = tAPI.setType('MCSimulation')
    self.assert_(res['OK'])
    res = tAPI.addTransformation()
    self.assert_(res['OK'])
    self.transID = res['Value']
    return res

  def test_SetGetReset(self):
    """ Testing of the set, get and reset methods.

          set*()
          get*()
          setTargetSE()
          setSourceSE()
          getTargetSE()
          getSourceSE()
          reset()
        Ensures that after a reset all parameters are returned to their defaults
    """
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

  def test_DeleteTransformation(self):
    """ Testing of the deletion of a transformation. 

          addTransformation()
          deleteTransformation()
           
        Tests that a transformation can be removed.
        Tests that retrieving a non existant transformation raises an AttributeError.
    """
    tAPI = Transformation(self.transID)
    res = tAPI.deleteTransformation()
    self.assert_(res['OK'])
    self.assertRaises(AttributeError,Transformation,self.transID)
    self.transID = 0

  def test_ExtendCleanTransformation(self):
    """ Tests the extension of transformations and the removal of tasks. Also obtain tasks, their status and update their status.
        
          extendTransformation()
          getTransformationTasks()
          getTransformationTaskStats()
          deleteTasks()
          setTaskStatus()
          cleanTransformation()
        
        Tests a transformation can be extended.
        Tests can obtain the the transformation tasks and their statistics.
        Tests the removal of already created tasks.
        Tests can change the status of a task.
        Tests that Cleaning a transformation removes tasks defined for the transformation.
    """
    tAPI = Transformation(self.transID)
    nTasks = 100
    res = tAPI.extendTransformation(nTasks)
    self.assert_(res['OK'])
    taskIDs = res['Value']
    self.assertEqual(len(taskIDs),nTasks)
    res = tAPI.getTransformationTasks()
    self.assert_(res['OK'])
    parameters = ['TargetSE', 'TransformationID', 'LastUpdateTime', 'JobWmsID', 'CreationTime', 'JobID', 'WmsStatus']
    self.assertEqual(sortList(res['ParameterNames']),sortList(parameters))
    self.assertEqual(sortList(res['Value'][0].keys()),sortList(parameters))
    self.assertEqual(res['Value'][0]['TargetSE'],'Unknown')
    self.assertEqual(res['Value'][0]['TransformationID'],self.transID)
    self.assertEqual(res['Value'][0]['JobWmsID'],'0')
    self.assertEqual(res['Value'][0]['JobID'],1)
    self.assertEqual(res['Value'][0]['WmsStatus'],'Created')
    self.assertEqual(res['Records'][0][0],1)
    self.assertEqual(res['Records'][0][1],self.transID)
    self.assertEqual(res['Records'][0][2],'Created')
    self.assertEqual(res['Records'][0][3],'0')
    self.assertEqual(res['Records'][0][4],'Unknown')
    res = tAPI.getTransformationTaskStats()
    self.assert_(res['OK'])
    self.assertEqual(res['Value']['Created'],100)
    res = tAPI.deleteTasks(11,100)
    self.assert_(res['OK'])
    res = tAPI.getTransformationTaskStats()
    self.assert_(res['OK'])
    self.assertEqual(res['Value']['Created'],10)
    res = tAPI.setTaskStatus(1, 'Done')
    self.assert_(res['OK'])
    res = tAPI.getTransformationTaskStats()
    self.assert_(res['OK'])
    self.assertEqual(res['Value']['Created'],10)
    self.assertEqual(res['Value']['Done'],1)
    res = tAPI.cleanTransformation()
    self.assert_(res['OK'])
    res = tAPI.getStatus()
    self.assert_(res['OK'])
    self.assertEqual(res['Value'],'Cleaned')
    res = tAPI.getTransformationTasks()
    self.assert_(res['OK'])
    self.assertFalse(res['Value'])
    self.assertFalse(res['Records'])

  def test_AddFilesGetFilesSetFileStatus(self):
    """ Testing adding, getting and setting file status.

          addFilesToTransformation()
          getTransformationFiles()
          getTransformationStats()
          setFileStatusForTransformation()
          addTaskForTransformation()

        Test adding and files to transformation.
        Test selecting the files for the transformation.
        Test getting the status count of the transformation files.
        Test setting the file status for transformation.
        Test creating a task for the added files and ensure the status is updated correctly.
    """
    tAPI = Transformation(self.transID)
    lfns = ['/test/lfn/file1','/test/lfn/file2']
    res = tAPI.addFilesToTransformation(lfns)
    self.assert_(res['OK'])
    res = tAPI.getTransformationFiles()
    self.assert_(res['OK'])
    self.assertEqual(sortList(lfns),res['LFNs'])
    self.assertEqual(len(lfns),len(res['Records']))
    self.assertEqual(len(lfns),len(res['Value']))
    fileParams = sortList(['LFN', 'TransformationID', 'FileID', 'Status', 'JobID', 'TargetSE', 'UsedSE', 'ErrorCount', 'LastUpdate', 'InsertedTime'])
    self.assertEqual(fileParams,sortList(res['ParameterNames']))
    self.assertEqual(res['Records'][0][0], lfns[0])
    self.assertEqual(res['Value'][0]['LFN'],lfns[0]) 
    self.assertEqual(res['Records'][0][1], self.transID)
    self.assertEqual(res['Value'][0]['TransformationID'], self.transID)
    self.assertEqual(res['Records'][0][3],'Unused')
    self.assertEqual(res['Value'][0]['Status'],'Unused')
    res = tAPI.getTransformationStats()
    self.assert_(res['OK'])
    self.assertEqual(res['Value']['Total'],2)
    self.assertEqual(res['Value']['Unused'],2)
    res = tAPI.setFileStatusForTransformation('Processed',[lfns[0]])
    self.assert_(res['OK'])
    res = tAPI.getTransformationStats()
    self.assert_(res['OK'])
    self.assertEqual(res['Value']['Total'],2)
    self.assertEqual(res['Value']['Unused'],1)
    self.assertEqual(res['Value']['Processed'],1)
    res = tAPI.setFileStatusForTransformation('Unused',[lfns[0]])
    self.assert_(res['OK'])
    self.assert_(res['Value']['Failed'].has_key(lfns[0]))
    self.assertEqual(res['Value']['Failed'][lfns[0]],'Can not change Processed status')
    res = tAPI.addTaskForTransformation(lfns=[lfns[1]],se='Test')
    self.assert_(res['OK'])
    res = tAPI.getTransformationStats()
    self.assert_(res['OK'])
    self.assertEqual(res['Value']['Total'],2)
    self.assertEqual(res['Value']['Assigned'],1)
    self.assertEqual(res['Value']['Processed'],1)
  
if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(APITestCase)  
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)

"""
tAPI.getTransformationLogging(printOutput=True)
tAPI.getTransformations(printOutput=True)

tAPI = Transformation()
tAPI.setTransformationName("TransformationTestName")
tAPI.setDescription("This is a short description")
tAPI.setLongDescription("This is a very long description isnt it")
tAPI.setType('Replication')
tAPI.setPlugin('Broadcast')
tAPI.setTargetSE(['GRIDKA_MC-DST'])
tAPI.setSourceSE(['CERN_MC_M-DST'])
print tAPI.generateBKQuery(test=True,printOutput=True)
res = tAPI.generateBkQuery(test=True,printOutput=True)
if not res['OK']:
  print res['Message']
else:
  bkQuery = res['Value']
  print bkQuery
  print tAPI.setBkQuery(bkQuery)
  print tAPI.getBkQuery()
  print tAPI.getBkQueryID()
  print tAPI.removeTransformationBkQuery()
"""
