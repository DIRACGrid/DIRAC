"""
This tests the new plugins for rel-v6r13: GFAL2_SRM2Storage and GFAL2_XROOTStorage.

Plugins:

To run the test following plugins are needed:

gfal2-2.9.3
gfal2-plugin-xrootd-0.4.0
gfal2-python-1.8.3

Storage element:
Change self.storageName within the basicTest class to the name of the storageElement you wish to run this test against.
This storage element needs to be supporting two access protocols, using the protocolName GFAL2_SRM2 and GFAL2_XROOT
respectively. If you want to test only either SRM2 or XROOT remove the other testsuite in the unittest.

"""


from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

import unittest
import pdb

from DIRAC import gLogger
from DIRAC.Resources.Storage.StorageElement import StorageElement

#### GLOBAL VARIABLES: ################

# Name of the storage element that has to be tested
STORAGE_NAME = 'CERN-GFAL2'

# base path on the storage where the test files/folders will be created
DESTINATION_PATH = '/lhcb/user/p/pgloor'

# local path containing test files. There should be a folder called Workflow containing (the files can be simple textfiles)
# FolderA
# -FolderAA
# --FileAA
# -FileA
# FolderB
# -FileB
# File1
# File2
# File3

LOCAL_PATH = 'UnitTests'

### END OF GLOBAL VARIABLES ###########


class basicTest( unittest.TestCase ):

  def setUp( self ):
    gLogger.setLevel( 'DEBUG' )

    self.storageName = STORAGE_NAME
    self.tbt = None

  def clearDirectory( self ):
    workflow_folder = DESTINATION_PATH + '/Workflow'
    res = self.tbt.removeDirectory( workflow_folder )
    if not res['OK']:
      print "basicTest.clearDirectory: Workflow folder maybe not empty"


  def testWorkflow( self ):

    putDir = {DESTINATION_PATH + '/Workflow/FolderA' : LOCAL_PATH + '/Workflow/FolderA', \
              DESTINATION_PATH + '/Workflow/FolderB' : LOCAL_PATH + '/Workflow/FolderB'}

    createDir = [DESTINATION_PATH + '/Workflow/FolderA/FolderAA', DESTINATION_PATH + '/Workflow/FolderA/FolderABA', \
                 DESTINATION_PATH + '/Workflow/FolderA/FolderAAB' ]

    putFile = {DESTINATION_PATH + '/Workflow/FolderA/File1' : LOCAL_PATH + '/Workflow/File1', \
               DESTINATION_PATH + '/Workflow/FolderAA/File1': LOCAL_PATH + '/Workflow/File1', \
               DESTINATION_PATH + '/Workflow/FolderBB/File2': LOCAL_PATH + '/Workflow/File2', \
               DESTINATION_PATH + '/Workflow/FolderB/File2' : LOCAL_PATH + '/Workflow/File2', \
               DESTINATION_PATH + '/Workflow/File3' : LOCAL_PATH + '/Workflow/File3' }

    isFile = [DESTINATION_PATH + '/Workflow/FolderA/File1', DESTINATION_PATH + '/Workflow/FolderB/FileB']


    listDir = [DESTINATION_PATH + '/Workflow', \
               DESTINATION_PATH + '/Workflow/FolderA', \
               DESTINATION_PATH + '/Workflow/FolderB']

    getDir = [DESTINATION_PATH + '/Workflow/FolderA', \
           DESTINATION_PATH + '/Workflow/FolderB']

    removeFile = [DESTINATION_PATH + '/Workflow/FolderA/File1']
    rmdir = [DESTINATION_PATH + '/Workflow']


    ########## uploading directory #############
    res = self.tbt.putDirectory( putDir )
    self.assertEqual( res['OK'], True )
    res = self.tbt.listDirectory( listDir )
    self.assertEqual( any( DESTINATION_PATH + '/Workflow/FolderA/FileA' in dictKey for dictKey in \
                  res['Value']['Successful'][DESTINATION_PATH + '/Workflow/FolderA']['Files'].keys() ), True )
    self.assertEqual( any( DESTINATION_PATH + '/Workflow/FolderB/FileB' in dictKey for dictKey in \
                      res['Value']['Successful'][DESTINATION_PATH + '/Workflow/FolderB']['Files'].keys() ), True )


    ########## createDir #############
    res = self.tbt.createDirectory( createDir )
    self.assertEqual( res['OK'], True )
    res = res['Value']
    self.assertEqual( res['Successful'][createDir[0]], True )
    self.assertEqual( res['Successful'][createDir[1]], True )
    self.assertEqual( res['Successful'][createDir[2]], True )

    ######## putFile ########
    res = self.tbt.putFile( putFile )
    self.assertEqual( res['OK'], True )

    res = self.tbt.isFile( isFile )
    self.assertEqual( res['OK'], True )
    self.assertEqual( res['Value']['Successful'][isFile[0]], True )
    self.assertEqual( res['Value']['Successful'][isFile[1]], True )

    ######## getMetadata ###########
    res = self.tbt.getFileMetadata( isFile )
    self.assertEqual( res['OK'], True )
    res = res['Value']['Successful']
    self.assertEqual( any( path in resKey for path in isFile for resKey in res.keys() ), True )

    ####### getDirectory ######
    res = self.tbt.getDirectory( getDir, LOCAL_PATH + '/getDir' )
    self.assertEqual( res['OK'], True )
    res = res['Value']
    self.assertEqual( any( getDir[0] in dictKey for dictKey in res['Successful'] ), True )
    self.assertEqual( any( getDir[1] in dictKey for dictKey in res['Successful'] ), True )

    ###### removeFile ##########
    res = self.tbt.removeFile( removeFile )
    self.assertEqual( res['OK'], True )
    res = self.tbt.exists( removeFile )
    self.assertEqual( res['OK'], True )
    self.assertEqual( res['Value']['Successful'][removeFile[0]], False )

    ###### remove non existing file #####
    res = self.tbt.removeFile( removeFile )
    self.assertEqual( res['OK'], True )
    res = self.tbt.exists( removeFile )
    self.assertEqual( res['OK'], True )
    self.assertEqual( res['Value']['Successful'][removeFile[0]], False )

    ########### removing directory  ###########
    res = self.tbt.removeDirectory( rmdir, True )
    res = self.tbt.exists( rmdir )
    self.assertEqual( res['OK'], True )
    self.assertEqual( res['Value']['Successful'][rmdir[0]], False )

class SRM2V2Test( basicTest ):

  def setUp( self ):
    basicTest.setUp( self )
    self.tbt = StorageElement( self.storageName, protocols = 'GFAL2_SRM2' )
    basicTest.clearDirectory( self )

class HTTPTest( basicTest ):

  def setUp( self ):
    basicTest.setUp( self )
    self.tbt = StorageElement( self.storageName, protocols = 'GFAL2_HTTP' )
    basicTest.clearDirectory( self )

class XROOTTest( basicTest ):

  def setUp( self ):
    basicTest.setUp( self )
    self.tbt = StorageElement( self.storageName, protocols = 'GFAL2_XROOT' )
    basicTest.clearDirectory( self )


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( SRM2V2Test )
  # suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( XROOTTest ) )
  unittest.TextTestRunner( verbosity = 2 ).run( suite )
