#! /usr/bin/env python
from DIRAC.Core.Base.Script                             import parseCommandLine
parseCommandLine()
from DIRAC.Resources.Catalog.FileCatalog                import FileCatalog
from DIRAC.Core.Utilities.File                          import makeGuid
from DIRAC.Core.Utilities.Adler                         import stringAdler
from types                                              import *
import unittest,time,os,shutil,sys

if len(sys.argv) < 2:
  print 'Usage: TestCatalogPlugIn.py CatalogClient'
  sys.exit()
else:
  catalogClientToTest = sys.argv[1]

class CatalogPlugInTestCase(unittest.TestCase):
  """ Base class for the CatalogPlugin test case """

  def setUp(self):
    self.catalog = FileCatalog(catalogs=[catalogClientToTest])
    valid = self.catalog.isOK()
    self.assert_(valid)
    self.destDir = '/lhcb/test/unit-test/TestCatalogPlugin'

    # Clean the existing directory
    self.cleanDirectory()
    res = self.catalog.createDirectory(self.destDir)
    returnValue = self.parseResult(res,self.destDir)

    # Register some files to work with
    self.numberOfFiles = 1
    self.files = []
    for i in range(self.numberOfFiles):
      lfn = "%s/testFile_%d" % (self.destDir,i)
      res = self.registerFile(lfn)
      self.assert_(res)
      self.files.append(lfn)

  def registerFile(self,lfn):
    pfn = 'protocol://host:port/storage/path%s' % lfn
    size = 10000000
    se = 'DIRAC-storage'
    guid = makeGuid()
    adler = stringAdler(guid)
    fileDict = {}
    fileDict[lfn] = {'PFN':pfn,'Size':size,'SE':se,'GUID':guid,'Checksum':adler}
    res = self.catalog.addFile(fileDict)
    return self.parseResult(res,lfn)

  def parseResult(self,res,path):
    self.assert_(res['OK'])
    self.assert_(res['Value'])
    self.assert_(res['Value']['Successful'])
    self.assert_(res['Value']['Successful'].has_key(path))
    return res['Value']['Successful'][path]

  def parseError(self,res,path):
    self.assert_(res['OK'])
    self.assert_(res['Value'])
    self.assert_(res['Value']['Failed'])
    self.assert_(res['Value']['Failed'].has_key(path))
    return res['Value']['Failed'][path]    

  def cleanDirectory(self):
    res = self.catalog.exists(self.destDir)
    returnValue = self.parseResult(res,self.destDir)
    if not returnValue:
      return
    res = self.catalog.listDirectory(self.destDir)  
    returnValue = self.parseResult(res,self.destDir)
    toRemove = returnValue['Files'].keys()
    if toRemove:
      self.purgeFiles(toRemove)
    res = self.catalog.removeDirectory(self.destDir)
    returnValue = self.parseResult(res,self.destDir)
    self.assert_(returnValue)

  def purgeFiles(self,lfns):
    for lfn in lfns:
      res = self.catalog.getReplicas(lfn,True)
      replicas = self.parseResult(res,lfn)
      for se,pfn in replicas.items():
        repDict = {}
        repDict[lfn] = {'PFN':pfn,'SE':se}
        res = self.catalog.removeReplica(repDict)
        self.parseResult(res,lfn)   
      res = self.catalog.removeFile(lfn)
      self.parseResult(res,lfn)

  def tearDown(self):
    self.cleanDirectory()

class DirectoryTestCase(CatalogPlugInTestCase):
  
  def test_isDirectory(self):
    res = self.catalog.isDirectory(self.destDir)
    returnValue = self.parseResult(res,self.destDir)
    self.assert_(returnValue)
    res = self.catalog.isDirectory(self.files[0])
    returnValue = self.parseResult(res,self.files[0])
    self.assertFalse(returnValue)
    res = self.catalog.isDirectory(self.files[0][:-1])
    error = self.parseError(res,self.files[0][:-1])
    self.assertEqual(error,"No such file or directory")

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(DirectoryTestCase)
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)

"""
class FilesCase(LFCClientTestCase):

  def test_files(self):

    ######################################################
    #
    # First create a file to use for remaining tests
    #
    lfn = '/lhcb/test/unit-test/testfile.%s' % time.time()
    pfn = 'protocol://host:port/storage/path%s' % lfn
    size = 10000000
    se = 'DIRAC-storage'
    guid = makeGuid()
    fileTuple = (lfn,pfn,size,se,guid)
    res = self.lfc.addFile(fileTuple)
    print res
    self.assert_(res['OK'])
    self.assert_(res['Value'].has_key('Successful'))
    self.assert_(res['Value'].has_key('Failed'))
    self.assert_(res['Value']['Successful'].has_key(lfn))
    self.assert_(res['Value']['Successful'][lfn])

    ######################################################
    #
    #  Test the creation of links using the test file
    #

    targetLfn = lfn
    linkName = '/lhcb/test/unit-test/testlink.%s' % time.time()

    linkTuple = (linkName,targetLfn)
    res = self.lfc.createLink(linkTuple)
    self.assert_(res['OK'])
    self.assert_(res['Value'].has_key('Successful'))
    self.assert_(res['Value'].has_key('Failed'))
    self.assert_(res['Value']['Successful'].has_key(linkName))
    self.assert_(res['Value']['Successful'][linkName])

    ######################################################
    #
    #  Test the recognition of links works (with file it should fail)
    #

    res = self.lfc.isLink(targetLfn)
    self.assert_(res['OK'])
    self.assert_(res['Value'].has_key('Successful'))
    self.assert_(res['Value'].has_key('Failed'))
    self.assert_(res['Value']['Successful'].has_key(targetLfn))
    self.assertFalse(res['Value']['Successful'][targetLfn])

    ######################################################
    #
    #  Test the recognition of links works (with link it shouldn't fail)
    #

    res = self.lfc.isLink(linkName)
    self.assert_(res['OK'])
    self.assert_(res['Value'].has_key('Successful'))
    self.assert_(res['Value'].has_key('Failed'))
    self.assert_(res['Value']['Successful'].has_key(linkName))
    self.assert_(res['Value']['Successful'][linkName])

    ######################################################
    #
    #  Test the resolution of links
    #

    res = self.lfc.readLink(linkName)
    self.assert_(res['OK'])
    self.assert_(res['Value'].has_key('Successful'))
    self.assert_(res['Value'].has_key('Failed'))
    self.assert_(res['Value']['Successful'].has_key(linkName))
    self.assertEqual(res['Value']['Successful'][linkName],targetLfn)

    ######################################################
    #
    #  Test the removal of links
    #

    res = self.lfc.removeLink(linkName)
    self.assert_(res['OK'])
    self.assert_(res['Value'].has_key('Successful'))
    self.assert_(res['Value'].has_key('Failed'))
    self.assert_(res['Value']['Successful'].has_key(linkName))
    self.assert_(res['Value']['Successful'][linkName])

    ######################################################
    #
    #  Test the recognition of non existant links
    #

    res = self.lfc.isLink(linkName)
    self.assert_(res['OK'])
    self.assert_(res['Value'].has_key('Successful'))
    self.assert_(res['Value'].has_key('Failed'))
    self.assert_(res['Value']['Failed'].has_key(linkName))
    self.assertEqual(res['Value']['Failed'][linkName],'No such file or directory')

    ######################################################
    #
    #  Add a replica to the test file
    #

    replicaPfn = 'protocol://replicaHost:port/storage/path%s' % lfn
    replicase = 'Replica-storage'
    replicaTuple = (lfn,replicaPfn,replicase,0)
    res = self.lfc.addReplica(replicaTuple)
    self.assert_(res['OK'])
    self.assert_(res['Value'].has_key('Successful'))
    self.assert_(res['Value'].has_key('Failed'))
    self.assert_(res['Value']['Successful'].has_key(lfn))
    self.assert_(res['Value']['Successful'][lfn])

    ######################################################
    #
    #  Ensure the file exists (quite redundant here)
    #

    res = self.lfc.exists(lfn)
    self.assert_(res['OK'])
    self.assert_(res['Value'].has_key('Successful'))
    self.assert_(res['Value'].has_key('Failed'))
    self.assert_(res['Value']['Successful'].has_key(lfn))
    self.assert_(res['Value']['Successful'][lfn])

    ######################################################
    #
    #  Test the recognition of files
    #

    res = self.lfc.isFile(lfn)
    self.assert_(res['OK'])
    self.assert_(res['Value'].has_key('Successful'))
    self.assert_(res['Value'].has_key('Failed'))
    self.assert_(res['Value']['Successful'].has_key(lfn))
    self.assert_(res['Value']['Successful'][lfn])

    ######################################################
    #
    #  Test obtaining the file metadata
    #

    res = self.lfc.getFileMetadata(lfn)
    self.assert_(res['OK'])
    self.assert_(res['Value'].has_key('Successful'))
    self.assert_(res['Value'].has_key('Failed'))
    self.assert_(res['Value']['Successful'].has_key(lfn))
    metadataDict = res['Value']['Successful'][lfn]
    self.assertEqual(metadataDict['Status'],'-')
    self.assertEqual(metadataDict['CheckSumType'],'')
    self.assertEqual(metadataDict['CheckSumValue'],'')
    self.assertEqual(metadataDict['Size'],10000000)

    ######################################################
    #
    #  Test obtaining the file replicas
    #

    res = self.lfc.getReplicas(lfn)
    self.assert_(res['OK'])
    self.assert_(res['Value'].has_key('Successful'))
    self.assert_(res['Value'].has_key('Failed'))
    self.assert_(res['Value']['Successful'].has_key(lfn))
    self.assert_(res['Value']['Successful'][lfn].has_key('DIRAC-storage'))
    self.assertEqual(res['Value']['Successful'][lfn]['DIRAC-storage'],pfn)
    self.assert_(res['Value']['Successful'][lfn].has_key('Replica-storage'))
    self.assertEqual(res['Value']['Successful'][lfn]['Replica-storage'],replicaPfn)

    ######################################################
    #
    #  Test obtaining the replica status for the master replica
    #

    replicaTuple = (lfn,pfn,se)
    res = self.lfc.getReplicaStatus(replicaTuple)
    self.assert_(res['OK'])
    self.assert_(res['Value'].has_key('Successful'))
    self.assert_(res['Value'].has_key('Failed'))
    self.assert_(res['Value']['Successful'].has_key(lfn))
    self.assertEqual(res['Value']['Successful'][lfn],'U')

    ######################################################
    #
    #  Test setting the replica status for the master replica
    #

    replicaTuple = (lfn,pfn,se,'C')
    res = self.lfc.setReplicaStatus(replicaTuple)
    self.assert_(res['OK'])
    self.assert_(res['Value'].has_key('Successful'))
    self.assert_(res['Value'].has_key('Failed'))
    self.assert_(res['Value']['Successful'].has_key(lfn))
    self.assert_(res['Value']['Successful'][lfn])

    ######################################################
    #
    #  Ensure the changing  of the replica status worked
    #

    replicaTuple = (lfn,pfn,se)
    res = self.lfc.getReplicaStatus(replicaTuple)
    self.assert_(res['OK'])
    self.assert_(res['Value'].has_key('Successful'))
    self.assert_(res['Value'].has_key('Failed'))
    self.assert_(res['Value']['Successful'].has_key(lfn))
    self.assertEqual(res['Value']['Successful'][lfn],'C')

    ######################################################
    #
    #  Test the change of storage element works
    #

    newse = 'New-storage'
    newToken = 'SpaceToken'
    replicaTuple = (lfn,pfn,newse,newToken)
    res = self.lfc.setReplicaHost(replicaTuple)
    self.assert_(res['OK'])
    self.assert_(res['Value'].has_key('Successful'))
    self.assert_(res['Value'].has_key('Failed'))
    self.assert_(res['Value']['Successful'].has_key(lfn))
    self.assert_(res['Value']['Successful'][lfn])

    ######################################################
    #
    #  Check the change of storage element works
    #

    res = self.lfc.getReplicas(lfn)
    self.assert_(res['OK'])
    self.assert_(res['Value'].has_key('Successful'))
    self.assert_(res['Value'].has_key('Failed'))
    self.assert_(res['Value']['Successful'].has_key(lfn))
    self.assert_(res['Value']['Successful'][lfn].has_key(newse))
    self.assertEqual(res['Value']['Successful'][lfn][newse],pfn)
    self.assert_(res['Value']['Successful'][lfn].has_key(replicase))
    self.assertEqual(res['Value']['Successful'][lfn][replicase],replicaPfn)

    ######################################################
    #
    #  Test getting the file size
    #

    res = self.lfc.getFileSize(lfn)
    self.assert_(res['OK'])
    self.assert_(res['Value'].has_key('Successful'))
    self.assert_(res['Value'].has_key('Failed'))
    self.assert_(res['Value']['Successful'].has_key(lfn))
    self.assertEqual(res['Value']['Successful'][lfn],size)

    ######################################################
    #
    #  Test getting replicas for directories
    #

    res = self.lfc.getDirectoryReplicas(dir)
    self.assert_(res['OK'])
    self.assert_(res['Value'].has_key('Successful'))
    self.assert_(res['Value'].has_key('Failed'))
    self.assert_(res['Value']['Successful'].has_key(dir))
    self.assert_(res['Value']['Successful'][dir].has_key(lfn))
    self.assert_(res['Value']['Successful'][dir][lfn].has_key(newse))
    self.assertEqual(res['Value']['Successful'][dir][lfn][newse],pfn)
    self.assert_(res['Value']['Successful'][dir][lfn].has_key(replicase))
    self.assertEqual(res['Value']['Successful'][dir][lfn][replicase],replicaPfn)

    ######################################################
    #
    #  Test listing directories
    #

    res = self.lfc.listDirectory(dir)
    self.assert_(res['OK'])
    self.assert_(res['Value'].has_key('Successful'))
    self.assert_(res['Value'].has_key('Failed'))
    self.assert_(res['Value']['Successful'].has_key(dir))
    self.assertEqual(res['Value']['Successful'][dir],[lfn])

    ######################################################
    #
    #  Test getting directory metadata
    #

    res = self.lfc.getDirectoryMetadata(dir)
    self.assert_(res['OK'])
    self.assert_(res['Value'].has_key('Successful'))
    self.assert_(res['Value'].has_key('Failed'))
    self.assert_(res['Value']['Successful'].has_key(dir))
    self.assert_(res['Value']['Successful'][dir].has_key('NumberOfSubPaths'))
    self.assertEqual(res['Value']['Successful'][dir]['NumberOfSubPaths'],1)
    self.assert_(res['Value']['Successful'][dir].has_key('CreationTime'))

    ######################################################
    #
    #  Test getting directory size
    #

    res = self.lfc.getDirectorySize(dir)
    self.assert_(res['OK'])
    self.assert_(res['Value'].has_key('Successful'))
    self.assert_(res['Value'].has_key('Failed'))
    self.assert_(res['Value']['Successful'].has_key(dir))
    self.assert_(res['Value']['Successful'][dir].has_key('Files'))
    self.assertEqual(res['Value']['Successful'][dir]['Files'],1)
    self.assert_(res['Value']['Successful'][dir].has_key('TotalSize'))
    self.assertEqual(res['Value']['Successful'][dir]['TotalSize'],size)
    self.assert_(res['Value']['Successful'][dir].has_key('SiteFiles'))
    self.assert_(res['Value']['Successful'][dir]['SiteFiles'].has_key(newse))
    self.assertEqual(res['Value']['Successful'][dir]['SiteFiles'][newse],1)
    self.assert_(res['Value']['Successful'][dir]['SiteFiles'].has_key(replicase))
    self.assertEqual(res['Value']['Successful'][dir]['SiteFiles'][replicase],1)
    self.assert_(res['Value']['Successful'][dir].has_key('SiteUsage'))
    self.assert_(res['Value']['Successful'][dir]['SiteUsage'].has_key(newse))
    self.assertEqual(res['Value']['Successful'][dir]['SiteUsage'][newse],size)
    self.assert_(res['Value']['Successful'][dir]['SiteUsage'].has_key(replicase))
    self.assertEqual(res['Value']['Successful'][dir]['SiteUsage'][replicase],size)

    ######################################################
    #
    #  Test creation of directories
    #

    newDir = '%s/%s' % (dir,'testDir')
    res = self.lfc.createDirectory(newDir)
    self.assert_(res['OK'])
    self.assert_(res['Value'].has_key('Successful'))
    self.assert_(res['Value'].has_key('Failed'))
    self.assert_(res['Value']['Successful'].has_key(newDir))
    self.assert_(res['Value']['Successful'][newDir])

    ######################################################
    #
    #  Test removal of directories
    #
    res = self.lfc.removeDirectory(newDir)
    self.assert_(res['OK'])
    self.assert_(res['Value'].has_key('Successful'))
    self.assert_(res['Value'].has_key('Failed'))
    self.assert_(res['Value']['Successful'].has_key(newDir))
    self.assert_(res['Value']['Successful'][newDir])

    ######################################################
    #
    #  Test removal of replicas
    #
    res = self.lfc.listDirectory(dir)
    self.assert_(res['OK'])
    self.assert_(res['Value'].has_key('Successful'))
    self.assert_(res['Value'].has_key('Failed'))
    self.assert_(res['Value']['Successful'].has_key(dir))
    lfnsToDelete = res['Value']['Successful'][dir]
    res = self.lfc.getReplicas(lfnsToDelete)
    self.assert_(res['OK'])
    self.assert_(res['Value'].has_key('Successful'))
    self.assert_(res['Value'].has_key('Failed'))
    replicas = res['Value']['Successful']
    replicaTupleList = []
    for lfn in replicas.keys():
      for se in replicas[lfn].keys():
        replicaTuple = (lfn,replicas[lfn][se],se)
        replicaTupleList.append(replicaTuple)

    res = self.lfc.removeReplica(replicaTupleList)
    self.assert_(res['OK'])
    self.assert_(res['Value'].has_key('Successful'))
    self.assert_(res['Value'].has_key('Failed'))
    self.assertFalse(res['Value']['Failed'])

    ######################################################
    #
    #  Test removal of files
    #

    res = self.lfc.removeFile(lfnsToDelete)
    self.assert_(res['OK'])
    self.assert_(res['Value'].has_key('Successful'))
    self.assert_(res['Value'].has_key('Failed'))
    self.assertFalse(res['Value']['Failed'])


"""
