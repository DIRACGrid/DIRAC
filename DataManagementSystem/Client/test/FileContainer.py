#! /usr/bin/env python
from DIRAC.DataManagementSystem.Client.FileContainer import FileContainer
import unittest,time,os,types

class FileContainerTestCase(unittest.TestCase):
  """ FileContainer base test case
  """
  def setUp(self):
    pass
  def tearDown(self):
    pass

class SimpleFileContainerTestCase(FileContainerTestCase):

  def test_defaults(self):
    oFile = FileContainer()
    attributesTypes =     { 'FileID'     :  types.IntType,
                            'Status'     :  types.StringType,
                            'LFN'        :  types.StringType,
                            'PFN'        :  types.StringType,
                            'Size'       :  types.IntType,
                            'GUID'       :  types.StringType,
                            'Md5'        :  types.StringType,
                            'Adler'      :  types.StringType,
                            'Attempt'    :  types.IntType,
                            'Error'      :  types.StringType}
    for attribute in attributesTypes.keys():
      execString = "res = oFile.get%s()" % attribute
      exec(execString) 
      self.assert_(res['OK'])
      self.assertEqual(type(res['Value']),attributesTypes[attribute])

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
