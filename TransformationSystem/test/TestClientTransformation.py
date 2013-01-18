#! /usr/bin/env python
from DIRAC.TransformationSystem.Client.Transformation   import Transformation
from DIRAC.Core.Utilities.List                          import sortList
import unittest,types,time,re

class TestClientTransformationTestCase(unittest.TestCase):

  def setUp(self):
    self.transID = 0
    transName = 'TestClientTransformation-%s' % time.strftime("%Y%m%d-%H:%M:%S")
    res = self._createTransformation(transName)
    self.assert_(res['OK'])
    self.transID = res['Value']

  def tearDown(self):
    if self.transID:
      oTrans = Transformation(self.transID)
      oTrans.deleteTransformation()

  def _createTransformation(self,transName,plugin='Standard'):
    oTrans = Transformation()
    res = oTrans.setTransformationName(transName)
    self.assert_(res['OK'])
    description = 'Test transforamtion description'
    res = oTrans.setDescription(description)
    longDescription = 'Test transformation long description'
    res = oTrans.setLongDescription(longDescription)
    self.assert_(res['OK'])
    res = oTrans.setType('MCSimulation')
    self.assert_(res['OK'])
    res = oTrans.setPlugin(plugin)
    self.assert_(res['OK'])
    res = oTrans.addTransformation()
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
    oTrans = Transformation()
    res = oTrans.getParameters()
    self.assert_(res['OK'])
    defaultParams = res['Value'].copy()
    for parameterName, defaultValue in res['Value'].items():
      if type(defaultValue) in types.StringTypes:
        testValue = "'TestValue'"
      else:
        testValue = '99999'
      ## set*

      setterName = "set%s" % parameterName
      self.assertEqual( hasattr( oTrans, setterName ), True )
      setter = getattr( oTrans, setterName )
      self.asserEqual( callable(setter), True )
      res = setter( testValue )
      self.assertEqual( res["OK"], True )
      ## get*
      getterName = "get%s" % parameterName 
      self.assertEqual( hasattr( oTrans, getterName ), True )
      getter = getattr( oTrans, setterName )
      self.asserEqual( callable(getter), True )
      res = getter() 
      self.asserEqual( res["OK"], True )
      self.asserEqual( res["Value"], eval(testValue) )
      
    # Test that SEs delivered as a space or comma seperated string are resolved...
    stringSEs = 'CERN-USER, CNAF-USER GRIDKA-USER,IN2P3-USER'
    listSEs = stringSEs.replace(',',' ').split()
    res = oTrans.setTargetSE(stringSEs)
    self.assert_(res['OK']) 
    res = oTrans.getTargetSE()
    self.assert_(res['OK'])
    self.assertEqual(res['Value'],listSEs)
    # ...but that lists are correctly handled also
    res = oTrans.setSourceSE(listSEs)
    self.assert_(res['OK']) 
    res = oTrans.getSourceSE()
    self.assert_(res['OK'])
    self.assertEqual(res['Value'],listSEs)

    res = oTrans.reset()
    self.assert_(res['OK'])
    res = oTrans.getParameters()
    self.assert_(res['OK'])
    for parameterName, resetValue in res['Value'].items():
      self.assertEqual(resetValue,defaultParams[parameterName])
    self.assertRaises(AttributeError,oTrans.getTargetSE)
    self.assertRaises(AttributeError,oTrans.getSourceSE)

  def test_DeleteTransformation(self):
    """ Testing of the deletion of a transformation. 

          addTransformation()
          deleteTransformation()
           
        Tests that a transformation can be removed.
        Tests that retrieving a non existant transformation raises an AttributeError.
    """
    oTrans = Transformation(self.transID)
    res = oTrans.deleteTransformation()
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
    oTrans = Transformation(self.transID)
    nTasks = 100
    res = oTrans.extendTransformation(nTasks)
    self.assert_(res['OK'])
    taskIDs = res['Value']
    self.assertEqual(len(taskIDs),nTasks)
    res = oTrans.getTransformationTasks()
    self.assert_(res['OK'])
    parameters = ['TargetSE', 'TransformationID', 'LastUpdateTime', 'ExternalID', 'CreationTime', 'TaskID', 'ExternalStatus']
    self.assertEqual(sortList(res['ParameterNames']),sortList(parameters))
    self.assertEqual(sortList(res['Value'][0].keys()),sortList(parameters))
    self.assertEqual(res['Value'][0]['TargetSE'],'Unknown')
    self.assertEqual(res['Value'][0]['TransformationID'],self.transID)
    self.assertEqual(res['Value'][0]['ExternalID'],'0')
    self.assertEqual(res['Value'][0]['TaskID'],1)
    self.assertEqual(res['Value'][0]['ExternalStatus'],'Created')
    self.assertEqual(res['Records'][0][0],1)
    self.assertEqual(res['Records'][0][1],self.transID)
    self.assertEqual(res['Records'][0][2],'Created')
    self.assertEqual(res['Records'][0][3],'0')
    self.assertEqual(res['Records'][0][4],'Unknown')
    res = oTrans.getTransformationTaskStats()
    self.assert_(res['OK'])
    self.assertEqual(res['Value']['Created'],100)
    res = oTrans.deleteTasks(11,100)
    self.assert_(res['OK'])
    res = oTrans.getTransformationTaskStats()
    self.assert_(res['OK'])
    self.assertEqual(res['Value']['Created'],10)
    res = oTrans.setTaskStatus(1, 'Done')
    self.assert_(res['OK'])
    res = oTrans.getTransformationTaskStats()
    self.assert_(res['OK'])
    self.assertEqual(res['Value']['Created'],10)
    self.assertEqual(res['Value']['Done'],1)
    res = oTrans.cleanTransformation()
    self.assert_(res['OK'])
    res = oTrans.getStatus()
    self.assert_(res['OK'])
    self.assertEqual(res['Value'],'Cleaned')
    res = oTrans.getTransformationTasks()
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
    oTrans = Transformation(self.transID)
    lfns = ['/test/lfn/file1','/test/lfn/file2']
    res = oTrans.addFilesToTransformation(lfns)
    self.assert_(res['OK'])
    res = oTrans.getTransformationFiles()
    self.assert_(res['OK'])
    self.assertEqual(sortList(lfns),res['LFNs'])
    self.assertEqual(len(lfns),len(res['Records']))
    self.assertEqual(len(lfns),len(res['Value']))
    fileParams = sortList(['LFN', 'TransformationID', 'FileID', 'Status', 'TaskID', 'TargetSE', 'UsedSE', 'ErrorCount', 'LastUpdate', 'InsertedTime'])
    self.assertEqual(fileParams,sortList(res['ParameterNames']))
    self.assertEqual(res['Records'][0][0], lfns[0])
    self.assertEqual(res['Value'][0]['LFN'],lfns[0]) 
    self.assertEqual(res['Records'][0][1], self.transID)
    self.assertEqual(res['Value'][0]['TransformationID'], self.transID)
    self.assertEqual(res['Records'][0][3],'Unused')
    self.assertEqual(res['Value'][0]['Status'],'Unused')
    res = oTrans.getTransformationStats()
    self.assert_(res['OK'])
    self.assertEqual(res['Value']['Total'],2)
    self.assertEqual(res['Value']['Unused'],2)
    res = oTrans.setFileStatusForTransformation('Processed',[lfns[0]])
    self.assert_(res['OK'])
    res = oTrans.getTransformationStats()
    self.assert_(res['OK'])
    self.assertEqual(res['Value']['Total'],2)
    self.assertEqual(res['Value']['Unused'],1)
    self.assertEqual(res['Value']['Processed'],1)
    res = oTrans.setFileStatusForTransformation('Unused',[lfns[0]])
    self.assert_(res['OK'])
    self.assert_(res['Value']['Failed'].has_key(lfns[0]))
    self.assertEqual(res['Value']['Failed'][lfns[0]],'Can not change Processed status')
    res = oTrans.addTaskForTransformation(lfns=[lfns[1]],se='Test')
    self.assert_(res['OK'])
    res = oTrans.getTransformationStats()
    self.assert_(res['OK'])
    self.assertEqual(res['Value']['Total'],2)
    self.assertEqual(res['Value']['Assigned'],1)
    self.assertEqual(res['Value']['Processed'],1)
 
  def test_getTransformations(self):  
    """ Testing the selection of transformations from the database

          getTransformations
         
        This will select all the transformations associated to this test suite and remove them.
    """
    oTrans = Transformation()
    res = oTrans.getTransformations()
    self.assert_(res['OK'])
    parameters = ['TransformationID', 'TransformationName', 'Description', 'LongDescription', 'CreationDate', 'LastUpdate', 'AuthorDN', 'AuthorGroup', 'Type', 'Plugin', 'AgentType', 'Status', 'FileMask', 'TransformationGroup', 'GroupSize', 'InheritedFrom', 'Body', 'MaxNumberOfTasks', 'EventsPerTask']
    self.assertEqual(sortList(res['ParameterNames']),sortList(parameters))
    self.assertEqual(sortList(res['Value'][0].keys()),sortList(parameters))
    self.assertEqual(len(res['Value']),len(res['Records']))
    ignore = self.transID
    for transDict in res['Value']:
      name = transDict['TransformationName']
      if re.search('TestClientTransformation',name):
        transID = transDict['TransformationID']
        if transID != ignore:
          oTrans = Transformation(transID)
          res = oTrans.deleteTransformation()
          self.assert_(res['OK'])
    self.transID = ignore

  def test_getTransformationLogging(self):
    """ Testing the obtaining of transformation logging information
     
          getTransformationLogging()
    """
    oTrans = Transformation(self.transID)
    res = oTrans.setStatus('Active')
    self.assert_(res['OK'])
    res = oTrans.extendTransformation(100)
    self.assert_(res['OK'])
    res = oTrans.setStatus('Completed')
    self.assert_(res['OK'])
    res = oTrans.cleanTransformation()
    self.assert_(res['OK'])
    res = oTrans.getTransformationLogging()
    self.assert_(res['OK'])
    self.assertEqual(len(res['Value']),6)
 
if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(TestClientTransformationTestCase)  
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
