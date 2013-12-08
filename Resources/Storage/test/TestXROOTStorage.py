''' XROOTStorage_TestCase

'''

from DIRAC.Core.Base.Script import parseCommandLine
from mock import MagicMock
parseCommandLine()

import mock
import unittest

import DIRAC.Resources.Storage.XROOTStorage as moduleTested
from DIRAC                                      import  gLogger


__RCSID__ = '$Id: $'

################################################################################

class XROOTStorage_TestCase( unittest.TestCase ):

  def setUp( self ):
    '''
    Setup
    '''
    # Mock external libraries / modules not interesting for the unit test
    mock_xrootlib = mock.Mock()
    mock_xrootlib.client.FileSystem.return_value( '' )
    self.mock_xrootlib = mock_xrootlib

    # Add mocks to moduleTested
    moduleTested.client = self.mock_xrootlib

    self.moduleTested = moduleTested
    self.testClass = self.moduleTested.XROOTStorage

  def tearDown( self ):
    '''
    TearDown
    '''
    del self.testClass
    del self.moduleTested




class xrootStatusMock:

  def __init__( self, message = "", ok = False, error = False, fatal = False, status = "", code = 0, shellcode = 0, errno = 0 ):
    self.message = message
    self.ok = ok
    self.error = error
    self.fatal = fatal
    self.status = status
    self.code = code
    self.shellcode = shellcode
    self.errno = errno

  def makeOk( self ):
    self.ok = True
    self.error = False
    self.fatal = False

  def makeError( self ):
    self.ok = False
    self.error = True
    self.errno = 1
    self.message = "I have an error"

  def makeFatal( self ):
    self.ok = False
    self.error = False
    self.fatal = True
    self.errno = 2
    self.message = "I am dead!"

def enum( **enums ):
  """Build the equivalent of a C++ enum"""
  reverse = dict( ( value, key ) for key, value in enums.iteritems() )
  enums['reverse_mapping'] = reverse
  return type( 'Enum', (), enums )



class xrootStatInfoMock:

  StatInfoFlags = enum( 
  X_BIT_SET    = 1,
  IS_DIR       = 2,
  OTHER        = 4,
  OFFLINE      = 8,
  IS_READABLE  = 16,
  IS_WRITABLE  = 32,
  POSC_PENDING = 64
  )

  def __init__( self, ModTime = 0, ModTimeStr = "never", Id = 0, Size = 0, Executable = False,
               Directory = False, Other = False, Offline = False, PoscPending = False,
               Readable = False, Writable = False):

    self.modtime = ModTime
    self.modtimestr = ModTimeStr
    self.id = Id
    self.size = Size


    self.ALL = 127

    flags = 0
    
    if Executable:
      flags |= xrootStatInfoMock.StatInfoFlags.X_BIT_SET
    if Directory:
      flags |= xrootStatInfoMock.StatInfoFlags.IS_DIR
    if Other:
      flags |= xrootStatInfoMock.StatInfoFlags.OTHER
    if Offline:
      flags |= xrootStatInfoMock.StatInfoFlags.OFFLINE
    if PoscPending:
      flags |= xrootStatInfoMock.StatInfoFlags.POSC_PENDING
    if Readable:
      flags |= xrootStatInfoMock.StatInfoFlags.IS_READABLE
    if Writable:
      flags |= xrootStatInfoMock.StatInfoFlags.IS_WRITABLE
    

    self.flags = flags
    
  def makeDir( self ):
    """ Set the other bit to false, and the dir bit to true """
    self.flags &= ~xrootStatInfoMock.StatInfoFlags.OTHER
    self.flags |= xrootStatInfoMock.StatInfoFlags.IS_DIR
    
  def makeFile( self ):
    """ set the other and dir bits to false"""
    self.flags &= ~xrootStatInfoMock.StatInfoFlags.OTHER
    self.flags &= ~xrootStatInfoMock.StatInfoFlags.IS_DIR



class xrootListEntryMock:
  
  def __init__(self, name = "name", hostaddr = "hostaddr", statinfo = None):
    self.name = name
    self.hostaddr = hostaddr
    self.statinfo = statinfo
    

class xrootDirectoryListMock:
  
  def __init__(self, parent = "parent", dirlist = []):
    self.size = len(dirlist)
    self.parent= parent
    self.dirlist = dirlist
    
  def __iter__( self ):
    return iter( self.dirlist )



################################################################################
# Tests

class XROOTStorage_Success( XROOTStorage_TestCase ):

  def test_instantiate( self ):
    ''' tests that we can instantiate one object of the tested class
    '''

    resource = self.testClass( 'storageName', 'protocol', 'path', 'host', 'port', 'spaceToken', 'wspath' )
    self.assertEqual( 'XROOTStorage', resource.__class__.__name__ )

  def test_init( self ):
    ''' tests that the init method does what it should do
    '''

    resource = self.testClass( 'storageName', 'protocol', 'path', 'host', 'port', 'spaceToken', 'wspath' )

    self.assertEqual( 'storageName', resource.name )
    self.assertEqual( 'XROOT' , resource.protocolName )
    self.assertEqual( 'protocol'   , resource.protocol )
    self.assertEqual( 'path'       , resource.rootdir )
    self.assertEqual( 'host'       , resource.host )
    self.assertEqual( ''       , resource.port )
    self.assertEqual( '' , resource.spaceToken )
    self.assertEqual( ''     , resource.wspath )

  def test_getParameters( self ):
    ''' tests the output of getParameters method
    '''

    resource = self.testClass( 'storageName', 'protocol', 'path', 'host', 'port', 'spaceToken', 'wspath' )

    res = resource.getParameters()
    self.assertEqual( True, res['OK'] )
    res = res[ 'Value' ]

    self.assertEqual( 'storageName', res['StorageName'] )
    self.assertEqual( 'XROOT' , res['ProtocolName'] )
    self.assertEqual( 'protocol'   , res['Protocol'] )
    self.assertEqual( 'path'       , res['Path'] )
    self.assertEqual( 'host'       , res['Host'] )
    self.assertEqual( ''       , res['Port'] )
    self.assertEqual( '' , res['SpaceToken'] )
    self.assertEqual( ''     , res['WSUrl'] )

  def test_getProtocolPfn( self ):
    ''' tests the output of getProtocolPfn
    '''

    resource = self.testClass( 'storageName', 'protocol', '/rootdir', 'host', 'port', 'spaceToken', 'wspath' )
    pfnDict = {}
    pfnDict['Protocol'] = 'root'
    pfnDict['Host'] = 'host'
    pfnDict['Port'] = 'port'
    pfnDict['WSUrl'] = 'WSUrl'
    pfnDict['Path'] = '/subpath'
    pfnDict['FileName'] = 'fileName'

    res = resource.getProtocolPfn( pfnDict, False )
    self.assertEqual( True, res['OK'] )
    res = res[ 'Value' ]
    self.assertEqual( "root://host//rootdir/subpath/fileName", res )

    res = resource.getProtocolPfn( pfnDict, True )
    self.assertEqual( True, res['OK'] )
    res = res[ 'Value' ]
    self.assertEqual( "root://host//rootdir/subpath/fileName", res )
    
    

  def test_getFileSize( self ):
    ''' tests the output of getFileSize
    '''

    resource = self.testClass( 'storageName', 'protocol', 'path', 'host', 'port', 'spaceToken', 'wspath' )
    
    statusMock = xrootStatusMock()
    statusMock.makeOk()
        
    statInfoMock = xrootStatInfoMock()
    statInfoMock.makeFile()
    statInfoMock.size = 10
    
    resource.xrootClient.stat.return_value = ( statusMock, statInfoMock )

    res = resource.getFileSize( 1 )
    self.assertEqual( False, res['OK'] )

    res = resource.getFileSize( {} )
    self.assertEqual( True, res['OK'] )

    res = resource.getFileSize( [] )
    self.assertEqual( True, res['OK'] )
   

    res = resource.getFileSize( [ 'A', 'B' ] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( {'A':10, 'B':10}, res['Value']['Successful'] )
    self.assertEqual( {}, res['Value']['Failed'] )

    res = resource.getFileSize( { 'A' : 1, 'B' : {}} )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( {'A':10, 'B':10}, res['Value']['Successful'] )
    self.assertEqual( {}, res['Value']['Failed'] )



  def test_getTransportURL( self):
    """ Test the transportURL method"""

    resource = self.testClass( 'storageName', 'protocol', 'path', 'host', 'port', 'spaceToken', 'wspath' )

    res = resource.getTransportURL( {} )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( {}, res['Value']['Successful'] )
    self.assertEqual( {}, res['Value']['Failed'] )

    res = resource.getTransportURL( {"A" : 0} )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( {"A" : "A"}, res['Value']['Successful'] )
    self.assertEqual( {}, res['Value']['Failed'] )

    res = resource.getTransportURL( {"A" : 0}, "protocol" )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( {"A" : "A"}, res['Value']['Successful'] )
    self.assertEqual( {}, res['Value']['Failed'] )
     
    res = resource.getTransportURL( {"A" : 0}, ["protocol", "other"] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( {"A" : "A"}, res['Value']['Successful'] )
    self.assertEqual( {}, res['Value']['Failed'] )
    
    
  def test_createDirectory( self):
    """ Test the create directory  method"""

    resource = self.testClass( 'storageName', 'protocol', 'path', 'host', 'port', 'spaceToken', 'wspath' )

    res = resource.createDirectory( {} )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( {}, res['Value']['Successful'] )
    self.assertEqual( {}, res['Value']['Failed'] )

    res = resource.createDirectory( {"A" : 0} )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( {"A" : True}, res['Value']['Successful'] )
    self.assertEqual( {}, res['Value']['Failed'] )

    res = resource.createDirectory( "A" )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( {"A" : True}, res['Value']['Successful'] )
    self.assertEqual( {}, res['Value']['Failed'] )
    
    
    
  def test_exists( self ):
    """ Test the existance of files and directories"""
    
    resource = self.testClass( 'storageName', 'protocol', 'path', 'host', 'port', 'spaceToken', 'wspath' )
    
    statusMock = xrootStatusMock()
    statusMock.makeOk()
        
    statInfoMock = xrootStatInfoMock()
    statInfoMock.makeFile()
    statInfoMock.size = 10
    
    resource.xrootClient.stat.return_value = ( statusMock, statInfoMock )
    
    
    # This test should be successful and True
    res = resource.exists( {"A" : 0} )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( {"A" : True}, res['Value']['Successful'] )
    self.assertEqual( {}, res['Value']['Failed'] )
    

    # This test should be successful and False (does not exist)
    statusMock.makeError()
    statusMock.errno = 3011

    res = resource.exists( {"A" : 0} )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( {"A" : False}, res['Value']['Successful'] )
    self.assertEqual( {}, res['Value']['Failed'] )

    # This test should be in Failed
    statusMock.makeError()
    statusMock.errno = 0

    res = resource.exists( {"A" : 0} )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( {}, res['Value']['Successful'] )
    self.assertEqual( "A", res['Value']['Failed'].keys()[0] )



  def test_getCurrentURL( self):
    """ Test the current URL of a file"""
    
    resource = self.testClass( 'storageName', 'protocol', 'path', 'host', 'port', 'spaceToken', 'wspath' )
    
    res = resource.getCurrentURL( "filename" )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( "protocol://host/path/filename", res['Value'] )

    
  def test_getDirectoryMetadata( self ):
    "Try to get the metadata of a directory"

    resource = self.testClass( 'storageName', 'protocol', 'path', 'host', 'port', 'spaceToken', 'wspath' )

    statusMock = xrootStatusMock()
    statusMock.makeOk()


    statInfoMock = xrootStatInfoMock()
    statInfoMock.makeDir()
    statInfoMock.size = 10

    resource.xrootClient.stat.return_value = ( statusMock, statInfoMock )

    # This test should be successful and True
    res = resource.getDirectoryMetadata( "A" )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( {}, res['Value']['Failed'] )

    metaDict = res['Value']['Successful']["A"]
    self.assertEqual( metaDict["Size"] , 10 )
    
    # We try on a file now, it should fail
    statInfoMock.makeFile()

    # This test should be successful and True
    res = resource.getDirectoryMetadata( "A" )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( {}, res['Value']['Successful'] )
    self.assertEqual( "A" , res['Value']['Failed'].keys()[0] )


  def test_list_listDirectory( self ):
    """ Try to list the directory"""


    resource = self.testClass( 'storageName', 'protocol', 'path', 'host', 'port', 'spaceToken', 'wspath' )

    statusMock = xrootStatusMock()
    statusMock.makeOk()

    statInfoMock = xrootStatInfoMock()
    statInfoMock.makeDir()

    resource.xrootClient.stat.return_value = ( statusMock, statInfoMock )


    statDir1 = xrootStatInfoMock()
    statDir1.makeDir()
    statDir1.size = 1
    dir1 = xrootListEntryMock( "dir1", "host", statDir1 )

    statDir2 = xrootStatInfoMock()
    statDir2.makeDir()
    statDir2.size = 2
    dir2 = xrootListEntryMock( "dir2", "host", statDir2 )

    statFile1 = xrootStatInfoMock()
    statFile1.makeFile()
    statFile1.size = 4
    file1 = xrootListEntryMock( "file1", "host", statFile1 )

    directoryListMock = xrootDirectoryListMock( "parent", [dir1, dir2, file1] )

    resource.xrootClient.dirlist.return_value = ( statusMock, directoryListMock )

    # We created a Directory which contains 2 subdir and 1 file

    res = resource.listDirectory( "A" )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( {}, res['Value']['Failed'] )
    SubDirs = res['Value']['Successful']["A"]["SubDirs"]
    SubFiles = res['Value']['Successful']["A"]["Files"]
    self.assertEqual( 2 , len( SubDirs ) )
    self.assertEqual( 1 , len( SubFiles ) )
    self.assertEqual( SubFiles["root://host/A/file1"]["Size"], 4 )


    # Let's try on a File. It should fail
    statInfoMock.makeFile()

    
    res = resource.listDirectory( "A" )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( {}, res['Value']['Successful'] )
    self.assertEqual( "A" , res['Value']['Failed'].keys()[0] )


  def test_getFileMetadata( self ):
    "Try to get the metadata of a File"

    resource = self.testClass( 'storageName', 'protocol', 'path', 'host', 'port', 'spaceToken', 'wspath' )

    statusMock = xrootStatusMock()
    statusMock.makeOk()


    statInfoMock = xrootStatInfoMock()
    statInfoMock.makeFile()
    statInfoMock.size = 10

    resource.xrootClient.stat.return_value = ( statusMock, statInfoMock )

    # This test should be successful and True
    res = resource.getFileMetadata( "A" )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( {}, res['Value']['Failed'] )

    metaDict = res['Value']['Successful']["A"]
    self.assertEqual( metaDict["Size"] , 10 )

    # We try on a directory now, it should fail
    statInfoMock.makeDir()

    # This test should be successful and True
    res = resource.getFileMetadata( "A" )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( {}, res['Value']['Successful'] )
    self.assertEqual( "A" , res['Value']['Failed'].keys()[0] )




  def test_getDirectorySize( self ):
    ''' tests the output of getDirectorySize
    '''


    resource = self.testClass( 'storageName', 'protocol', 'path', 'host', 'port', 'spaceToken', 'wspath' )

    statusMock = xrootStatusMock()
    statusMock.makeOk()

    statInfoMock = xrootStatInfoMock()
    statInfoMock.makeDir()

    resource.xrootClient.stat.return_value = ( statusMock, statInfoMock )


    statDir1 = xrootStatInfoMock()
    statDir1.makeDir()
    statDir1.size = 1
    dir1 = xrootListEntryMock( "dir1", "host", statDir1 )


    statFile1 = xrootStatInfoMock()
    statFile1.makeFile()
    statFile1.size = 4
    file1 = xrootListEntryMock( "file1", "host", statFile1 )

    directoryListMock = xrootDirectoryListMock( "parent", [dir1, file1] )

    resource.xrootClient.dirlist.return_value = ( statusMock, directoryListMock )


    # We have 1 file (size4) and 1 subdir in the directory
    res = resource.getDirectorySize( 'A' )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( 1, res['Value']['Successful']["A"]["Files"] )
    self.assertEqual( 1, res['Value']['Successful']["A"]["SubDirs"] )
    self.assertEqual( 4, res['Value']['Successful']["A"]["Size"] )
    self.assertEqual( {}, res['Value']['Failed'] )


  def test_isDirectory( self ):
    """ Check if a path is a directory"""

    resource = self.testClass( 'storageName', 'protocol', 'path', 'host', 'port', 'spaceToken', 'wspath' )

    statusMock = xrootStatusMock()
    statusMock.makeOk()


    statInfoMock = xrootStatInfoMock()
    statInfoMock.makeDir()

    resource.xrootClient.stat.return_value = ( statusMock, statInfoMock )

    # This test should be successful and True
    res = resource.isDirectory( "A" )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( {}, res['Value']['Failed'] )
    self.assertEqual( {"A" : True}, res['Value']['Successful'] )

    statInfoMock.makeFile()

    # This test should be successful and False
    res = resource.isDirectory( "A" )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( {}, res['Value']['Failed'] )
    self.assertEqual( {"A" : False}, res['Value']['Successful'] )


  def test_isFile( self ):
    """ Check if a path is a File"""

    resource = self.testClass( 'storageName', 'protocol', 'path', 'host', 'port', 'spaceToken', 'wspath' )

    statusMock = xrootStatusMock()
    statusMock.makeOk()


    statInfoMock = xrootStatInfoMock()
    statInfoMock.makeFile()

    resource.xrootClient.stat.return_value = ( statusMock, statInfoMock )

    # This test should be successful and True
    res = resource.isFile( "A" )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( {}, res['Value']['Failed'] )
    self.assertEqual( {"A" : True}, res['Value']['Successful'] )
    
    # This test should be successful and True
    statusMock.makeError()
    res = resource.isFile( "A" )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( {}, res['Value']['Successful'] )
    self.assertEqual( "A", res['Value']['Failed'].keys()[0] )
    

    # This test should return S_ERROR
    statusMock.makeFatal()
    res = resource.isFile( "A" )
    self.assertEqual( False, res['OK'] )

    
    statusMock.makeOk()
    statInfoMock.makeDir()

    # This test should be successful and False
    res = resource.isFile( "A" )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( {}, res['Value']['Failed'] )
    self.assertEqual( {"A" : False}, res['Value']['Successful'] )
    
    # This test should be successful and True
    statusMock.makeError()
    res = resource.isFile( "A" )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( {}, res['Value']['Successful'] )
    self.assertEqual( "A", res['Value']['Failed'].keys()[0] )
    

    # This test should return S_ERROR
    statusMock.makeFatal()
    res = resource.isFile( "A" )
    self.assertEqual( False, res['OK'] )


    
  def test_removeFile( self ):
    ''' tests the output of removeFile
    '''


    resource = self.testClass( 'storageName', 'protocol', 'path', 'host', 'port', 'spaceToken', 'wspath' )

    statusMock = xrootStatusMock()
    statusMock.makeOk()

    resource.xrootClient.rm.return_value = ( statusMock, None )

    # This test should be successful and True
    res = resource.removeFile( "A" )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( {"A" : True}, res['Value']['Successful'] )
    self.assertEqual( {}, res['Value']['Failed'] )


    # This test should be successful and True (file was not there, so it is successfully deleted...)
    statusMock.makeError()
    statusMock.errno = 3011
    res = resource.removeFile( "A" )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( {"A" : True}, res['Value']['Successful'] )
    self.assertEqual( {}, res['Value']['Failed'] )

    # This test should be in Failed
    statusMock.makeError()
    statusMock.errno = 0
    res = resource.removeFile( "A" )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( {}, res['Value']['Successful'] )
    self.assertEqual( "A", res['Value']['Failed'].keys()[0] )

    # This should return S_ERROR
    statusMock.makeFatal()
    res = resource.removeFile( "A" )
    self.assertEqual( False, res['OK'] )


  def test_removeDirectory( self ):
    ''' tests the output of removeDirectory
    '''

    resource = self.testClass( 'storageName', 'protocol', 'path', 'host', 'port', 'spaceToken', 'wspath' )

    statusStatDirMock = xrootStatusMock()
    statusStatDirMock.makeOk()

    statInfoMock = xrootStatInfoMock()
    statInfoMock.makeDir()

    resource.xrootClient.stat.return_value = ( statusStatDirMock, statInfoMock )


    statDir1 = xrootStatInfoMock()
    statDir1.makeDir()
    statDir1.size = 1
    dir1 = xrootListEntryMock( "dir1", "host", statDir1 )

    statDir2 = xrootStatInfoMock()
    statDir2.makeDir()
    statDir2.size = 2
    dir2 = xrootListEntryMock( "dir2", "host", statDir2 )

    statFile1 = xrootStatInfoMock()
    statFile1.makeFile()
    statFile1.size = 4
    file1 = xrootListEntryMock( "file1", "host", statFile1 )

    statFile2 = xrootStatInfoMock()
    statFile2.makeFile()
    statFile2.size = 8
    file2 = xrootListEntryMock( "file2", "host", statFile2 )

    statFile3 = xrootStatInfoMock()
    statFile3.makeFile()
    statFile3.size = 16
    file3 = xrootListEntryMock( "file3", "host", statFile3 )

    directoryListMock1 = xrootDirectoryListMock( "parent", [dir1, dir2, file1] )
    directoryListMock2 = xrootDirectoryListMock( "dir1", [file2] )
    directoryListMock3 = xrootDirectoryListMock( "dir1", [file3] )


    
    statusMock = xrootStatusMock()
    statusMock.makeOk()
    resource.xrootClient.rm.return_value = ( statusMock, None )
    resource.xrootClient.rmdir.return_value = ( statusMock, None )

    # This test should remove file1 only
    resource.xrootClient.dirlist.return_value = ( statusStatDirMock, directoryListMock1 )
    res = resource.removeDirectory( "A", recursive = False )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( {"A" : { "FilesRemoved" : 1, "SizeRemoved" : 4}}, res['Value']['Successful'] )
    self.assertEqual( {}, res['Value']['Failed'] )

    resource.xrootClient.dirlist = MagicMock( side_effect = [( statusStatDirMock, directoryListMock1 ), ( statusStatDirMock, directoryListMock2 ), ( statusStatDirMock, directoryListMock3 )] )

    # This test should remove the 3 files
    res = resource.removeDirectory( "A", recursive = True )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( {"A" : { "FilesRemoved" : 3, "SizeRemoved" : 28}}, res['Value']['Successful'] )
    self.assertEqual( {}, res['Value']['Failed'] )

    # The rmdir command fails
    statusMock.makeError()
    resource.xrootClient.dirlist = MagicMock( side_effect = [( statusStatDirMock, directoryListMock1 )] )
    res = resource.removeDirectory( "A", recursive = False )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( {}, res['Value']['Successful'] )
    self.assertEqual( {"A" : { "FilesRemoved" : 0, "SizeRemoved" : 0}}, res['Value']['Failed'] )

    # The rmdir command is fatal
    statusMock.makeFatal()
    resource.xrootClient.dirlist = MagicMock( side_effect = [( statusStatDirMock, directoryListMock1 )] )
    res = resource.removeDirectory( "A", recursive = False )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( {}, res['Value']['Successful'] )
    self.assertEqual( {"A" : { "FilesRemoved" : 0, "SizeRemoved" : 0}}, res['Value']['Failed'] )


  def test_getFile( self ):
    """ Test the output of getFile"""

    resource = self.testClass( 'storageName', 'protocol', 'path', 'host', 'port', 'spaceToken', 'wspath' )

    statusMock = xrootStatusMock()
    statusMock.makeOk()

    resource.xrootClient.copy.return_value = ( statusMock, None )

    statusStatMock = xrootStatusMock()
    statusStatMock.makeOk()


    statInfoMock = xrootStatInfoMock()
    statInfoMock.makeFile()
    statInfoMock.size = -1

    resource.xrootClient.stat.return_value = ( statusStatMock, statInfoMock )


    # This test should be completely okay
    res = resource.getFile( "a", "/tmp" )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( {"a" :-1}, res['Value']['Successful'] )
    self.assertEqual( {}, res['Value']['Failed'] )


    # Here the sizes should not match
    statInfoMock.size = 1000
    res = resource.getFile( "a", "/tmp" )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( {}, res['Value']['Successful'] )
    self.assertEqual( "a", res['Value']['Failed'].keys()[0] )
    statInfoMock.size = -1


    # Here we should not be able to get the file from storage
    statusMock.makeError()
    res = resource.getFile( "a", "/tmp" )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( {}, res['Value']['Successful'] )
    self.assertEqual( "a", res['Value']['Failed'].keys()[0] )

    # Fatal error in getting the file from storage
    statusMock.makeFatal()
    res = resource.getFile( "a", "/tmp" )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( {}, res['Value']['Successful'] )
    self.assertEqual( "a", res['Value']['Failed'].keys()[0] )


  def test_getDirectory( self ):
    ''' tests the output of getDirectory
    '''
    resource = self.testClass( 'storageName', 'protocol', 'path', 'host', 'port', 'spaceToken', 'wspath' )

    statusStatDirMock = xrootStatusMock()
    statusStatDirMock.makeOk()

    statInfoMockDir = xrootStatInfoMock()
    statInfoMockDir.makeDir()

    statInfoMockFile = xrootStatInfoMock()
    statInfoMockFile.size = -1
    statInfoMockFile.makeFile()

    # This dirty thing forces us to know how many time api.stat is called and in what order...
    resource.xrootClient.stat = MagicMock( side_effect = [( statusStatDirMock, statInfoMockDir ),
                                                          ( statusStatDirMock, statInfoMockFile ),
                                                          ( statusStatDirMock, statInfoMockDir ),
                                                          ( statusStatDirMock, statInfoMockFile ),
                                                          ( statusStatDirMock, statInfoMockDir ),
                                                          ( statusStatDirMock, statInfoMockFile )] )


    statDir1 = xrootStatInfoMock()
    statDir1.makeDir()
    statDir1.size = -1
    dir1 = xrootListEntryMock( "dir1", "host", statDir1 )

    statDir2 = xrootStatInfoMock()
    statDir2.makeDir()
    statDir2.size = -1
    dir2 = xrootListEntryMock( "dir2", "host", statDir2 )

    statFile1 = xrootStatInfoMock()
    statFile1.makeFile()
    statFile1.size = -1
    file1 = xrootListEntryMock( "file1", "host", statFile1 )

    statFile2 = xrootStatInfoMock()
    statFile2.makeFile()
    statFile2.size = -1
    file2 = xrootListEntryMock( "file2", "host", statFile2 )

    statFile3 = xrootStatInfoMock()
    statFile3.makeFile()
    statFile3.size = -1
    file3 = xrootListEntryMock( "file3", "host", statFile3 )

    directoryListMock1 = xrootDirectoryListMock( "parent", [dir1, dir2, file1] )
    directoryListMock2 = xrootDirectoryListMock( "dir1", [file2] )
    directoryListMock3 = xrootDirectoryListMock( "dir1", [file3] )



    statusMock = xrootStatusMock()
    statusMock.makeOk()
    resource.xrootClient.copy.return_value = ( statusMock, None )


    resource.xrootClient.dirlist = MagicMock( side_effect = [( statusStatDirMock, directoryListMock1 ), ( statusStatDirMock, directoryListMock2 ), ( statusStatDirMock, directoryListMock3 )] )

    # This test should get the 3 files
    res = resource.getDirectory( "A" )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( {"A" : { "Files" : 3, "Size" :-3}}, res['Value']['Successful'] )
    self.assertEqual( {}, res['Value']['Failed'] )

    # The copy command is just in error
    statusMock.makeError()
    resource.xrootClient.dirlist = MagicMock( side_effect = [( statusStatDirMock, directoryListMock1 ), ( statusStatDirMock, directoryListMock2 ), ( statusStatDirMock, directoryListMock3 )] )
    resource.xrootClient.stat = MagicMock( side_effect = [( statusStatDirMock, statInfoMockDir ),
                                                          ( statusStatDirMock, statInfoMockFile ),
                                                          ( statusStatDirMock, statInfoMockDir ),
                                                          ( statusStatDirMock, statInfoMockFile ),
                                                          ( statusStatDirMock, statInfoMockDir ),
                                                          ( statusStatDirMock, statInfoMockFile )] )
    res = resource.getDirectory( "A" )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( {}, res['Value']['Successful'] )
    self.assertEqual( {"A" : { "Files" : 0, "Size" : 0}}, res['Value']['Failed'] )


    # The copy command is fatal
    statusMock.makeFatal()
    resource.xrootClient.dirlist = MagicMock( side_effect = [( statusStatDirMock, directoryListMock1 ), ( statusStatDirMock, directoryListMock2 ), ( statusStatDirMock, directoryListMock3 )] )
    resource.xrootClient.stat = MagicMock( side_effect = [( statusStatDirMock, statInfoMockDir ),
                                                          ( statusStatDirMock, statInfoMockFile ),
                                                          ( statusStatDirMock, statInfoMockDir ),
                                                          ( statusStatDirMock, statInfoMockFile ),
                                                          ( statusStatDirMock, statInfoMockDir ),
                                                          ( statusStatDirMock, statInfoMockFile )] )
    res = resource.getDirectory( "A" )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( {}, res['Value']['Successful'] )
    self.assertEqual( {"A" : { "Files" : 0, "Size" : 0}}, res['Value']['Failed'] )



  def test_putFile( self ):
    """ Test the output of putFile"""

    resource = self.testClass( 'storageName', 'protocol', 'path', 'host', 'port', 'spaceToken', 'wspath' )
    import os
    os.path.exists = MagicMock( return_value = True )

    getSize_mock = MagicMock( return_value = 1 )
    self.moduleTested.getSize = getSize_mock


    statusMock = xrootStatusMock()
    statusMock.makeOk()

    resource.xrootClient.copy.return_value = ( statusMock, None )

    statusMkDirMock = xrootStatusMock()
    statusMkDirMock.makeOk()

    resource.xrootClient.mkdir.return_value = ( statusMkDirMock, None )


    statusRmMock = xrootStatusMock()
    statusRmMock.makeOk()

    resource.xrootClient.rm.return_value = ( statusRmMock, None )


    statusStatMock = xrootStatusMock()
    statusStatMock.makeOk()


    statInfoMock = xrootStatInfoMock()
    statInfoMock.makeFile()
    statInfoMock.size = 1

    resource.xrootClient.stat.return_value = ( statusStatMock, statInfoMock )


    # This test should be completely okay
    res = resource.putFile( {"remoteA" : "localA"} )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( {"remoteA" : 1}, res['Value']['Successful'] )
    self.assertEqual( {}, res['Value']['Failed'] )


    # Here the sizes should not match
    statInfoMock.size = 1000
    res = resource.putFile( {"remoteA" : "localA"} )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( {}, res['Value']['Successful'] )
    self.assertEqual( "remoteA", res['Value']['Failed'].keys()[0] )
    statInfoMock.size = 1


    # Here we should not be able to get the file from storage
    statusMock.makeError()
    res = resource.putFile( {"remoteA" : "localA"} )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( {}, res['Value']['Successful'] )
    self.assertEqual( "remoteA", res['Value']['Failed'].keys()[0] )

    # Fatal error in getting the file from storage
    statusMock.makeFatal()
    res = resource.putFile( {"remoteA" : "localA"} )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( {}, res['Value']['Successful'] )
    self.assertEqual( "remoteA", res['Value']['Failed'].keys()[0] )


    # Bad input
    res = resource.putFile( "remoteA" )
    self.assertEqual( False, res['OK'] )


  def test_putDirectory( self ):
    ''' tests the output of putDirectory
    '''

    # I again try to have 2 subdirs, and 1 file per subdir and 1 file a the root

    resource = self.testClass( 'storageName', 'protocol', 'path', 'host', 'port', 'spaceToken', 'wspath' )
    import os
    os.path.isdir = MagicMock( side_effect = [True, True, True, False, True, True, False, False] )
    os.listdir = MagicMock( side_effect = [( "dir1", "dir2", "file1" ), ( "file2", ), ( "file3", )] )
    os.path.exists = MagicMock( return_value = True )

    getSize_mock = MagicMock( return_value = 1 )
    self.moduleTested.getSize = getSize_mock



    statusCopyMock = xrootStatusMock()
    statusCopyMock.makeOk()

    resource.xrootClient.copy.return_value = ( statusCopyMock, None )

    statusMkDirMock = xrootStatusMock()
    statusMkDirMock.makeOk()

    resource.xrootClient.mkdir.return_value = ( statusMkDirMock, None )

    statusRmMock = xrootStatusMock()
    statusRmMock.makeOk()

    resource.xrootClient.rm.return_value = ( statusRmMock, None )

    statusStatMock = xrootStatusMock()
    statusStatMock.makeOk()
    statInfoMock = xrootStatInfoMock()
    statInfoMock.makeFile()
    statInfoMock.size = 1

    resource.xrootClient.stat.return_value = ( statusStatMock, statInfoMock )


    # This test should upload the 3 files
    res = resource.putDirectory( {"remoteA" : "localA"} )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( {"remoteA" : { "Files" : 3, "Size" :3}}, res['Value']['Successful'] )
    self.assertEqual( {}, res['Value']['Failed'] )


    # The copy command is just in error
    statusCopyMock.makeError()
    os.path.isdir = MagicMock( side_effect = [True, True, True, False, True, True, False, False] )
    os.listdir = MagicMock( side_effect = [( "dir1", "dir2", "file1" ), ( "file2", ), ( "file3", )] )

    res = resource.putDirectory( {"remoteA" : "localA"} )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( {}, res['Value']['Successful'] )
    self.assertEqual( {"remoteA" : { "Files" : 0, "Size" : 0}}, res['Value']['Failed'] )


    # The copy command is fatal
    statusCopyMock.makeFatal()
    os.path.isdir = MagicMock( side_effect = [True, True, True, False, True, True, False, False] )
    os.listdir = MagicMock( side_effect = [( "dir1", "dir2", "file1" ), ( "file2", ), ( "file3", )] )

    res = resource.putDirectory( {"remoteA" : "localA"} )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( {}, res['Value']['Successful'] )
    self.assertEqual( {"remoteA" : { "Files" : 0, "Size" : 0}}, res['Value']['Failed'] )





if __name__ == '__main__':

  # Shut up the gLogger
  # Uncomment this line and comment the next 2 if you want debug
  # gLogger.setLevel( "DEBUG" )
  from DIRAC.FrameworkSystem.private.logging.Logger import Logger
  Logger.processMessage = MagicMock()


  suite = unittest.defaultTestLoader.loadTestsFromTestCase( XROOTStorage_Success )
  unittest.TextTestRunner( verbosity = 2 ).run( suite )


################################################################################
# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
