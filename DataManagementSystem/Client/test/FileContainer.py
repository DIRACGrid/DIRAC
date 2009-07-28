#! /usr/bin/env python
from DIRAC.DataManagementSystem.Client.FileContainer import FileContainer
import unittest,time,os,types

class FileContainerTestCase(unittest.TestCase):
  """ FileContainer base test case
  """
  def setUp(self):
    self.attributesTypes ={ 'FileID'     :  0,
                            'Status'     :  'Waiting',
                            'LFN'        :  '',
                            'PFN'        :  '',
                            'Size'       :  0,   
                            'GUID'       :  '',
                            'Md5'        :  '',
                            'Adler'      :  '',
                            'Attempt'    :  0,   
                            'Error'      :  ''}
  def tearDown(self):
    pass

class SimpleFileContainerTestCase(FileContainerTestCase):

  def test_defaults(self):
    oFile = FileContainer()
    for attribute in self.attributesTypes.keys():
      execString = "res = oFile.get%s()" % attribute
      exec(execString) 
      self.assert_(res['OK'])
      self.assertEqual(type(res['Value']),type(self.attributesTypes[attribute]))

  def test_update(self):
    oFile = FileContainer()
    badNewAttribute = []
    for attribute in self.attributesTypes.keys():
      execString = "res = oFile.set%s(%s)" % (attribute,badNewAttribute)
      exec(execString)
      self.assertFalse(res['OK'])
      if type(self.attributesTypes[attribute]) == types.IntType:
        execString = "res = oFile.set%s(%s)" % (attribute,self.attributesTypes[attribute])
      else:
        execString = "res = oFile.set%s('%s')" % (attribute,self.attributesTypes[attribute])
      exec(execString)
      self.assert_(res['OK'])
 
  def test_isEmpty(self):
    oFile = FileContainer()
    res = oFile.isEmpty()
    self.assert_(res['OK'])
    self.assertFalse(res['Value'])    
    oFile.setStatus('Done')
    res = oFile.isEmpty() 
    self.assert_(res['OK'])
    self.assert_(res['Value'])
    oFile.setStatus('Failed')
    res = oFile.isEmpty() 
    self.assert_(res['OK'])
    self.assert_(res['Value'])

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(SimpleFileContainerTestCase)
  testResult = unittest.TextTestRunner(verbosity=1).run(suite)
