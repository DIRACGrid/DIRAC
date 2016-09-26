"""
This integration tests will perform basic operations on a storage element, depending on which protocols are available.
It creates a local hierarchy, and then tries to upload, download, remove, get metadata etc

Note: it might seem a good idea to simply add tests for the old srm in it. It is not :-)
      There is a deadlock between gfal and gfal2 libraries, you can't load both of them together

Examples:
<python Test_Resources_GFAL2StorageBase.py CERN-GFAL2>: will test all the gfal2 plugins defined for CERN-GFAL2
<python Test_Resources_GFAL2StorageBase.py CERN-GFAL2 GFAL2_XROOT>: will test the GFAL2_XROOT plugins defined for CERN-GFAL2

"""


from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

import unittest
import sys
import os
import tempfile
import shutil

from DIRAC import gLogger
from DIRAC.Resources.Storage.StorageElement import StorageElement
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.ConfigurationSystem.Client.Helpers.Registry  import getVOForGroup

#### GLOBAL VARIABLES: ################

# Name of the storage element that has to be tested

if len( sys.argv ) < 2 :
  print "Usage: %s <SE name> <plugins>" % sys.argv[0]
  print "\t<SE name>: mandatory"
  print "\t <plugins>: comma separated list of plugin to test (defautl all)"
  sys.exit( 1 )

STORAGE_NAME = sys.argv[1]

# base path on the storage where the test files/folders will be created
DESTINATION_PATH = ''

try:
  res = getProxyInfo()
  if not res['OK']:
    gLogger.error( "Failed to get client proxy information.", res['Message'] )
    sys.exit( 2 )
  proxyInfo = res['Value']
  username = proxyInfo['username']
  vo = ''
  if 'group' in proxyInfo:
    vo = getVOForGroup( proxyInfo['group'] )

  DESTINATION_PATH = '/%s/user/%s/%s/gfaltests' % ( vo, username[0], username )

except Exception as e:  # pylint: disable=broad-except
  print repr( e )
  sys.exit( 2 )




if len( sys.argv ) == 3:
  AVAILABLE_PLUGINS = sys.argv[2].split( ',' )
else:
  AVAILABLE_PLUGINS = StorageElement( STORAGE_NAME ).getPlugins()['Value']


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

### END OF GLOBAL VARIABLES ###########


class basicTest( unittest.TestCase ):

  def setUp( self ):
    gLogger.setLevel( 'DEBUG' )
    self.LOCAL_PATH = tempfile.mkdtemp()

    self.storageName = STORAGE_NAME
    self.tbt = None

    # create the local structure
    workPath = os.path.join( self.LOCAL_PATH, 'Workflow' )
    os.mkdir( workPath )

    os.mkdir( os.path.join( workPath, 'FolderA' ) )
    with open( os.path.join( workPath, 'FolderA', 'FileA' ), 'w' ) as f:
      f.write( 'FileA' )

    os.mkdir( os.path.join( workPath, 'FolderA', 'FolderAA' ) )
    with open( os.path.join( workPath, 'FolderA', 'FolderAA', 'FileAA' ), 'w' ) as f:
      f.write( 'FileAA' )

    os.mkdir( os.path.join( workPath, 'FolderB' ) )
    with open( os.path.join( workPath, 'FolderB', 'FileB' ), 'w' ) as f:
      f.write( 'FileB' )

    for fn in ["File1", "File2", "File3"]:
      with open( os.path.join( workPath, fn ), 'w' ) as f:
        f.write( fn )

  def tearDown( self ):
    shutil.rmtree( self.LOCAL_PATH )
    self.clearDirectory()


  def clearDirectory( self ):
    print "=================================================="
    print "==== Removing the older Directory ================"
    workflow_folder = DESTINATION_PATH + '/Workflow'
    res = self.tbt.removeDirectory( workflow_folder )
    if not res['OK']:
      print "basicTest.clearDirectory: Workflow folder maybe not empty"
    print "=================================================="

  def testWorkflow( self ):

    putDir = {os.path.join( DESTINATION_PATH, 'Workflow/FolderA' ) : os.path.join( self.LOCAL_PATH, 'Workflow/FolderA' ),
              os.path.join( DESTINATION_PATH, 'Workflow/FolderB' ) : os.path.join( self.LOCAL_PATH, 'Workflow/FolderB' )
              }

    createDir = [os.path.join( DESTINATION_PATH, 'Workflow/FolderA/FolderAA' ),
                 os.path.join( DESTINATION_PATH, 'Workflow/FolderA/FolderABA' ),
                 os.path.join( DESTINATION_PATH, 'Workflow/FolderA/FolderAAB' )
                ]

    putFile = {os.path.join( DESTINATION_PATH, 'Workflow/FolderA/File1' ) : os.path.join( self.LOCAL_PATH, 'Workflow/File1' ),
               os.path.join( DESTINATION_PATH, 'Workflow/FolderAA/File1' ): os.path.join( self.LOCAL_PATH, 'Workflow/File1' ),
               os.path.join( DESTINATION_PATH, 'Workflow/FolderBB/File2' ): os.path.join( self.LOCAL_PATH, 'Workflow/File2' ),
               os.path.join( DESTINATION_PATH, 'Workflow/FolderB/File2' ) : os.path.join( self.LOCAL_PATH, 'Workflow/File2' ),
               os.path.join( DESTINATION_PATH, 'Workflow/File3' ) : os.path.join( self.LOCAL_PATH, 'Workflow/File3' )
              }

    isFile = [os.path.join( DESTINATION_PATH, 'Workflow/FolderA/File1' ),
              os.path.join( DESTINATION_PATH, 'Workflow/FolderB/FileB' )
             ]


    listDir = [os.path.join( DESTINATION_PATH, 'Workflow' ),
               os.path.join( DESTINATION_PATH, 'Workflow/FolderA' ),
               os.path.join( DESTINATION_PATH, 'Workflow/FolderB' )
              ]

    getDir = [os.path.join( DESTINATION_PATH, 'Workflow/FolderA' ),
              os.path.join( DESTINATION_PATH, 'Workflow/FolderB' )
             ]

    removeFile = [os.path.join( DESTINATION_PATH, 'Workflow/FolderA/File1' )]
    rmdir = [os.path.join( DESTINATION_PATH, 'Workflow' )]


    ########## uploading directory #############
    res = self.tbt.putDirectory( putDir )
    self.assertEqual( res['OK'], True )
    res = self.tbt.listDirectory( listDir )
    self.assertEqual( any( os.path.join( DESTINATION_PATH, 'Workflow/FolderA/FileA' ) in dictKey for dictKey in \
                  res['Value']['Successful'][os.path.join( DESTINATION_PATH, 'Workflow/FolderA' )]['Files'].keys() ), True )
    self.assertEqual( any( os.path.join( DESTINATION_PATH, 'Workflow/FolderB/FileB' ) in dictKey for dictKey in \
                      res['Value']['Successful'][os.path.join( DESTINATION_PATH , 'Workflow/FolderB' )]['Files'].keys() ), True )


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
    res = self.tbt.getDirectory( getDir, os.path.join( self.LOCAL_PATH, 'getDir' ) )
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

@unittest.skipIf( 'GFAL2_SRM2' not in AVAILABLE_PLUGINS,
                 "StorageElement %s does not have plugin GFAL2_SRM defined" % STORAGE_NAME )
class GFAL2_SRM2_Test( basicTest ):

  def setUp( self ):
    basicTest.setUp( self )
    self.tbt = StorageElement( self.storageName, plugins = 'GFAL2_SRM2' )
    basicTest.clearDirectory( self )

@unittest.skipIf( 'GFAL2_HTTP' not in AVAILABLE_PLUGINS,
                   "StorageElement %s does not have plugin GFAL2_HTTP defined" % STORAGE_NAME )
class GFAL2_HTTP_Test( basicTest ):

  def setUp( self ):
    basicTest.setUp( self )
    self.tbt = StorageElement( self.storageName, plugins = 'GFAL2_HTTP' )
    basicTest.clearDirectory( self )


@unittest.skipIf( 'GFAL2_XROOT' not in AVAILABLE_PLUGINS,
                  "StorageElement %s does not have plugin GFAL2_XROOT defined" % STORAGE_NAME )
class GFAL2_XROOT_Test( basicTest ):

  def setUp( self ):
    basicTest.setUp( self )
    self.tbt = StorageElement( self.storageName, plugins = 'GFAL2_XROOT' )
    basicTest.clearDirectory( self )

@unittest.skipIf( 'XROOT' not in AVAILABLE_PLUGINS,
                  "StorageElement %s does not have plugin XROOT defined" % STORAGE_NAME )
class XROOT_Test( basicTest ):

  def setUp( self ):
    basicTest.setUp( self )
    self.tbt = StorageElement( self.storageName, plugins = 'XROOT' )
    basicTest.clearDirectory( self )





if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( GFAL2_SRM2_Test )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( GFAL2_XROOT_Test ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( GFAL2_HTTP_Test ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( XROOT_Test ) )
  unittest.TextTestRunner( verbosity = 2 ).run( suite )

