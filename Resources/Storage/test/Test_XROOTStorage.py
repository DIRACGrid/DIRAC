''' XROOTStorage_TestCase

'''


import unittest
import sys
from mock import MagicMock

import mock

# Mock the import of xrootd in XROOTStorage
mocked_xrootd = MagicMock()
mocked_xrootclient = MagicMock()
mocked_xrootd.client.FileSystem.return_value = mocked_xrootclient
mocked_xrootclient.stat.return_value = None, None

sys.modules['XRootD'] = mocked_xrootd
sys.modules['XRootD.client'] = mocked_xrootd.client
sys.modules['XRootD.client.flags'] = mocked_xrootd.client.flags


def enum(**enums):
  """Build the equivalent of a C++ enum"""
  reverse = dict((value, key) for key, value in enums.iteritems())
  enums['reverse_mapping'] = reverse
  return type('Enum', (), enums)


StatInfoFlags = enum(
    X_BIT_SET=1,
    IS_DIR=2,
    OTHER=4,
    OFFLINE=8,
    IS_READABLE=16,
    IS_WRITABLE=32,
    POSC_PENDING=64,
    BACKUP_EXISTS=128
)

mocked_xrootd.client.flags.StatInfoFlags = StatInfoFlags

from DIRAC.Resources.Storage.XROOTStorage import XROOTStorage  # as moduleTested

from DIRAC import S_ERROR, S_OK

__RCSID__ = '$Id$'

################################################################################


class XROOTStorage_TestCase(unittest.TestCase):

  def setUp(self):
    '''
    Setup
    '''
    # Mock external libraries / modules not interesting for the unit test
    mock_xrootlib = mock.Mock()
    mock_xrootlib.client.FileSystem.return_value('')
    self.mock_xrootlib = mock_xrootlib

    sm = xrootStatusMock()
    sim = xrootStatInfoMock()
    # Needed to set initial value of stat.return_value
    global mocked_xrootclient
    mocked_xrootclient.stat.return_value = sm, sim
    sm.makeOk()
    updateStatMockReferences(sm, sim)
    # Fixes the need to call makeOk at the end of the removeFile test (Fatal flag used to survive the test(cleanup))
    mocked_xrootclient.rm.return_value = sm, sim
    mocked_xrootclient.rm.side_effect = None

    self.parameterDict = dict(Protocol='protocol',
                              Path='/path',
                              Host='host',
                              Port='',
                              SpaceToken='spaceToken',
                              WSPath='wspath',
                              )
    self.testClass = None

  def tearDown(self):
    '''
    TearDown
    '''
    del self.testClass
    # Reset side effects, since they override the return_values set in some
    # tests. (Scenario: Test 1 sets a side_effect, completes, side_effect is
    # still set in the mock. Test 2 sets a return value, is not used since
    # side_effect takes priority)
    global mocked_xrootclient
    mocked_xrootclient.stat.return_value = None
    mocked_xrootclient.stat.side_effect = None
    mocked_xrootclient.dirlist.return_value = None
    mocked_xrootclient.dirlist.side_effect = None
    mocked_xrootclient.rm.return_value = None
    mocked_xrootclient.rm.side_effect = None

# Mocks the first return value of a filesystem call (these return values
# are of the form (a,b) with a containing status information on the
# operations success and b containing information on the details of the
# operation (e.g. number of removed files/bytes in a removedir operation))


class xrootStatusMock:

  def __init__(self, message="", ok=False, error=False, fatal=False, status="", code=0, shellcode=0, errno=0):
    self.message = message
    self.ok = ok
    self.error = error
    self.fatal = fatal
    self.status = status
    self.code = code
    self.shellcode = shellcode
    self.errno = errno

  def makeOk(self):
    self.ok = True
    self.error = False
    self.fatal = False

  def makeError(self):
    self.ok = False
    self.error = True
    self.errno = 1
    self.message = "I have an error"

  def makeFatal(self):
    self.ok = False
    self.error = False
    self.fatal = True
    self.errno = 2
    self.message = "I am dead!"


class xrootStatInfoMock:

  StatInfoFlags = enum(X_BIT_SET=1,
                       IS_DIR=2,
                       OTHER=4,
                       OFFLINE=8,
                       IS_READABLE=16,
                       IS_WRITABLE=32,
                       POSC_PENDING=64
                       )

  def __init__(self, ModTime=0, ModTimeStr="never", Id=0, Size=0, Executable=False,
               Directory=False, Other=False, Offline=False, PoscPending=False,
               Readable=False, Writable=False):

    self.modtime = ModTime
    self.modtimestr = ModTimeStr
    self.id = Id
    self.size = Size

    self.ALL = 127

    flags = 0

    if Executable:
      flags |= xrootStatInfoMock.StatInfoFlags.X_BIT_SET  # pylint: disable=no-member
    if Directory:
      flags |= xrootStatInfoMock.StatInfoFlags.IS_DIR  # pylint: disable=no-member
    if Other:
      flags |= xrootStatInfoMock.StatInfoFlags.OTHER  # pylint: disable=no-member
    if Offline:
      flags |= xrootStatInfoMock.StatInfoFlags.OFFLINE  # pylint: disable=no-member
    if PoscPending:
      flags |= xrootStatInfoMock.StatInfoFlags.POSC_PENDING  # pylint: disable=no-member
    if Readable:
      flags |= xrootStatInfoMock.StatInfoFlags.IS_READABLE  # pylint: disable=no-member
    if Writable:
      flags |= xrootStatInfoMock.StatInfoFlags.IS_WRITABLE  # pylint: disable=no-member

    self.flags = flags

  def makeDir(self):
    """ Set the other bit to false, and the dir bit to true """
    self.flags &= ~xrootStatInfoMock.StatInfoFlags.OTHER  # pylint: disable=no-member
    self.flags |= xrootStatInfoMock.StatInfoFlags.IS_DIR  # pylint: disable=no-member
    mocked_xrootd.client.flags = self.flags
    updateStatMockReferences(infoval=self)

  def makeFile(self):
    """ set the other and dir bits to false"""
    self.flags &= ~xrootStatInfoMock.StatInfoFlags.OTHER  # pylint: disable=no-member
    self.flags &= ~xrootStatInfoMock.StatInfoFlags.IS_DIR  # pylint: disable=no-member
    mocked_xrootd.client.flags = self.flags
    updateStatMockReferences(infoval=self)


class xrootListEntryMock:

  def __init__(self, name="name", hostaddr="hostaddr", statinfo=None):
    self.name = name
    self.hostaddr = hostaddr
    self.statinfo = statinfo


class xrootDirectoryListMock:

  def __init__(self, parent="parent", dirlist=[]):
    self.size = len(dirlist)
    self.parent = parent
    self.dirlist = dirlist

  def __iter__(self):
    return iter(self.dirlist)


################################################################################
# Tests

class XROOTStorage_Success(XROOTStorage_TestCase):

  def setUp(self):
    super(XROOTStorage_Success, self).setUp()

  def tearDown(self):
    super(XROOTStorage_Success, self).tearDown()

  def test_instantiate(self):
    ''' tests that we can instantiate one object of the tested class
    '''

    resource = XROOTStorage('storageName', self.parameterDict)
    self.assertEqual('XROOTStorage', resource.__class__.__name__)

  def test_init(self):
    ''' tests that the init method does what it should do
    '''

    resource = XROOTStorage('storageName', self.parameterDict)

    self.assertEqual('storageName', resource.name)
    self.assertEqual('XROOT', resource.pluginName)
    self.assertEqual('protocol', resource.protocol)
    self.assertEqual('/path', resource.protocolParameters['Path'])
    self.assertEqual('host', resource.protocolParameters['Host'])
    self.assertEqual(0, resource.protocolParameters['Port'])
    self.assertEqual('spaceToken', resource.protocolParameters['SpaceToken'])
    self.assertEqual(0, resource.protocolParameters['WSUrl'])

  def test_getParameters(self):
    ''' tests the output of getParameters method
    '''

    resource = XROOTStorage('storageName', self.parameterDict)

    res = resource.getParameters()
    self.assertEqual('storageName', res['StorageName'])
    self.assertEqual('protocol', res['Protocol'])
    self.assertEqual('/path', res['Path'])
    self.assertEqual('host', res['Host'])
    self.assertEqual(0, res['Port'])
    self.assertEqual('spaceToken', res['SpaceToken'])
    self.assertEqual(0, res['WSUrl'])

  # Legacy (?)
  # def test_getProtocolPfn( self ):
  #   ''' tests the output of getProtocolPfn
  #   '''
  #   parameters = dict(self.parameterDict )
  #   parameters['Path'] = '/rootdir'
  #   #'protocol', '/rootdir', 'host', 'port', 'spaceToken', 'wspath'
  #   resource = self.testClass( 'storageName', parameters )
  #   pfnDict = {}
  #   pfnDict['Protocol'] = 'root'
  #   pfnDict['Host'] = 'host'
  #   pfnDict['Port'] = 'port'
  #   pfnDict['WSUrl'] = 'WSUrl'
  #   pfnDict['Path'] = '/subpath'
  #   pfnDict['FileName'] = 'fileName'

  #   res = resource.getProtocolPfn( pfnDict, False )
  #   self.assertEqual( True, res['OK'] )
  #   res = res[ 'Value' ]
  #   self.assertEqual( "root://host//rootdir/subpath/fileName", res )

  #   res = resource.getProtocolPfn( pfnDict, True )
  #   self.assertEqual( True, res['OK'] )
  #   res = res[ 'Value' ]
  #   self.assertEqual( "root://host//rootdir/subpath/fileName", res )

  def test_getFileSize(self):
    ''' tests the output of getFileSize
    '''

    resource = XROOTStorage('storageName', self.parameterDict)

    statusMock = xrootStatusMock()
    statusMock.makeOk()
    filesize_to_test = 136

    statInfoMock = xrootStatInfoMock()
    statInfoMock.makeFile()
    statInfoMock.size = filesize_to_test

    updateStatMockReferences(statusMock, statInfoMock)

    res = resource.getFileSize(1)
    self.assertEqual(False, res['OK'])

    res = resource.getFileSize({})
    self.assertEqual(True, res['OK'])

    res = resource.getFileSize([])
    self.assertEqual(True, res['OK'])

    res = resource.getFileSize('A')
    self.assertEqual(True, res['OK'])
    self.assertEqual({'A': filesize_to_test}, res['Value']['Successful'])

    res = resource.getFileSize(['A', 'B'])
    self.assertEqual(True, res['OK'])
    self.assertEqual({'A': filesize_to_test, 'B': filesize_to_test}, res['Value']['Successful'])
    self.assertEqual({}, res['Value']['Failed'])

    res = resource.getFileSize({'A': 1, 'B': {}})
    self.assertEqual(True, res['OK'])
    self.assertEqual({'A': filesize_to_test, 'B': filesize_to_test}, res['Value']['Successful'])
    self.assertEqual({}, res['Value']['Failed'])

  def test_getTransportURL(self):
    """ Test the transportURL method"""

    resource = XROOTStorage('storageName', self.parameterDict)

    res = resource.getTransportURL({})
    self.assertEqual(True, res['OK'])
    self.assertEqual({}, res['Value']['Successful'])
    self.assertEqual({}, res['Value']['Failed'])

    res = resource.getTransportURL({"A": 0})
    self.assertEqual(True, res['OK'])
    self.assertEqual({"A": "A"}, res['Value']['Successful'])
    self.assertEqual({}, res['Value']['Failed'])

    res = resource.getTransportURL({"A": 0}, "protocol")
    self.assertEqual(True, res['OK'])
    self.assertEqual({"A": "A"}, res['Value']['Successful'])
    self.assertEqual({}, res['Value']['Failed'])

    res = resource.getTransportURL({"A": 0}, ["protocol", "other"])
    self.assertEqual(True, res['OK'])
    self.assertEqual({"A": "A"}, res['Value']['Successful'])
    self.assertEqual({}, res['Value']['Failed'])

  def test_createDirectory(self):
    """ Test the create directory  method"""

    resource = XROOTStorage('storageName', self.parameterDict)

    res = resource.createDirectory({})
    self.assertEqual(True, res['OK'])
    self.assertEqual({}, res['Value']['Successful'])
    self.assertEqual({}, res['Value']['Failed'])

    res = resource.createDirectory({"A": 0})
    self.assertEqual(True, res['OK'])
    self.assertEqual({"A": True}, res['Value']['Successful'])
    self.assertEqual({}, res['Value']['Failed'])

    res = resource.createDirectory("A")
    self.assertEqual(True, res['OK'])
    self.assertEqual({"A": True}, res['Value']['Successful'])
    self.assertEqual({}, res['Value']['Failed'])

  def test_exists(self):
    """ Test the existance of files and directories"""

    resource = XROOTStorage('storageName', self.parameterDict)

    statusMock = xrootStatusMock()
    statusMock.makeOk()

    statInfoMock = xrootStatInfoMock()
    statInfoMock.makeFile()
    statInfoMock.size = 10

    updateStatMockReferences(statusMock, statInfoMock)

    # This test should be successful and True
    res = resource.exists({"A": 0})
    self.assertEqual(True, res['OK'])
    self.assertEqual({"A": True}, res['Value']['Successful'])
    self.assertEqual({}, res['Value']['Failed'])

    # This test should be successful and False (does not exist)
    statusMock.makeError()
    statusMock.errno = 3011

    res = resource.exists({"A": 0})
    self.assertEqual(True, res['OK'])
    self.assertEqual({"A": False}, res['Value']['Successful'])
    self.assertEqual({}, res['Value']['Failed'])

    # This test should be in Failed
    statusMock.makeError()
    statusMock.errno = 0

    res = resource.exists({"A": 0})
    self.assertEqual(True, res['OK'])
    self.assertEqual({}, res['Value']['Successful'])
    self.assertEqual("A", res['Value']['Failed'].keys()[0])

  def test_getCurrentURL(self):
    """ Test the current URL of a file"""

    resource = XROOTStorage('storageName', self.parameterDict)

    res = resource.getCurrentURL("filename")
    self.assertEqual(True, res['OK'])
    self.assertEqual("protocol://host//path/filename", res['Value'])

  def test_getDirectoryMetadata(self):
    "Try to get the metadata of a directory"

    resource = XROOTStorage('storageName', self.parameterDict)

    statusMock = xrootStatusMock()
    statusMock.makeOk()

    statInfoMock = xrootStatInfoMock()
    statInfoMock.makeDir()
    statInfoMock.size = 10

    updateStatMockReferences(statusMock, statInfoMock)

    # This test should be successful and True
    res = resource.getDirectoryMetadata("A")
    self.assertEqual(True, res['OK'])
    self.assertEqual({}, res['Value']['Failed'])

    metaDict = res['Value']['Successful']["A"]
    self.assertEqual(metaDict["Size"], 10)

    # We try on a file now, it should fail
    statInfoMock.makeFile()

    # This test should be successful and True
    res = resource.getDirectoryMetadata("A")
    self.assertEqual(True, res['OK'])
    self.assertEqual({}, res['Value']['Successful'])
    self.assertEqual("A", res['Value']['Failed'].keys()[0])

  def test_listDirectory(self):
    """ Try to list the directory"""
    global mocked_xrootclient

    resource = XROOTStorage('storageName', self.parameterDict)

    statusMock = xrootStatusMock()
    statusMock.makeOk()

    statInfoMock = xrootStatInfoMock()
    statInfoMock.makeDir()

    updateStatMockReferences(statusMock, statInfoMock)

    statDir1 = xrootStatInfoMock()
    statDir1.makeDir()
    statDir1.size = 1
    dir1 = xrootListEntryMock("dir1", "host", statDir1)

    statDir2 = xrootStatInfoMock()
    statDir2.makeDir()
    statDir2.size = 2
    dir2 = xrootListEntryMock("dir2", "host", statDir2)

    statFile1 = xrootStatInfoMock()
    statFile1.makeFile()
    statFile1.size = 4
    file1 = xrootListEntryMock("file1", "host", statFile1)

    directoryListMock = xrootDirectoryListMock("parent", [dir1, dir2, file1])

    parentdir = xrootStatInfoMock()
    parentdir.makeDir()

    setMockDirectory(directoryListMock)

    resource.se = MagicMock()
    voName = "A"
    resource.se.vo = voName
    # We created a Directory which contains 2 subdir and 1 file

    aUrl = resource.constructURLFromLFN("/A")['Value']
    res = resource.listDirectory(aUrl)
    self.assertEqual(True, res['OK'])
    self.assertEqual({}, res['Value']['Failed'])
    SubDirs = res['Value']['Successful'][aUrl]["SubDirs"]
    SubFiles = res['Value']['Successful'][aUrl]["Files"]
    self.assertEqual(2, len(SubDirs))
    self.assertEqual(1, len(SubFiles))
    self.assertEqual(SubFiles["/A/file1"]["Size"], 4)

    # Cleanup old side effect
    mocked_xrootclient.stat.side_effect = None

    # Let's try on a File. It should fail
    statInfoMock.makeFile()
    updateStatMockReferences(infoval=statInfoMock)

    res = resource.listDirectory(aUrl)
    self.assertEqual(True, res['OK'])
    self.assertEqual({}, res['Value']['Successful'])
    self.assertEqual(aUrl, res['Value']['Failed'].keys()[0])

  def test_getFileMetadata(self):
    "Try to get the metadata of a File"

    global mocked_xrootclient

    resource = XROOTStorage('storageName', self.parameterDict)

    statusMock = xrootStatusMock()
    statusMock.makeOk()

    statInfoMock = xrootStatInfoMock()
    statInfoMock.makeFile()
    statInfoMock.size = 10

    updateStatMockReferences(statusMock, statInfoMock)

    # This test should be successful and True
    res = resource.getFileMetadata("A")
    self.assertEqual(True, res['OK'])
    self.assertEqual({}, res['Value']['Failed'])

    metaDict = res['Value']['Successful']["A"]
    self.assertEqual(metaDict["Size"], 10)

    # We try on a directory now, it should fail
    statInfoMock.makeDir()

    # This test should be successful and True
    res = resource.getFileMetadata("A")
    self.assertEqual(True, res['OK'])
    self.assertEqual({}, res['Value']['Successful'])
    self.assertEqual("A", res['Value']['Failed'].keys()[0])

  def test_getDirectorySize(self):
    ''' tests the output of getDirectorySize
    '''

    resource = XROOTStorage('storageName', self.parameterDict)

    statusMock = xrootStatusMock()
    statusMock.makeOk()

    statInfoMock = xrootStatInfoMock()
    statInfoMock.makeDir()

    updateStatMockReferences(statusMock, statInfoMock)

    statDir1 = xrootStatInfoMock()
    statDir1.makeDir()
    statDir1.size = 1
    dir1 = xrootListEntryMock("dir1", "host", statDir1)

    statFile1 = xrootStatInfoMock()
    statFile1.makeFile()
    statFile1.size = 4
    file1 = xrootListEntryMock("file1", "host", statFile1)

    directoryListMock = xrootDirectoryListMock("parent", [dir1, file1])

    mocked_xrootclient.dirlist.return_value = (statusMock, directoryListMock)

    # We have 1 file (size4) and 1 subdir in the directory
    res = resource.getDirectorySize('A')
    self.assertEqual(True, res['OK'])
    self.assertEqual(1, res['Value']['Successful']["A"]["Files"])
    self.assertEqual(1, res['Value']['Successful']["A"]["SubDirs"])
    self.assertEqual(4, res['Value']['Successful']["A"]["Size"])
    self.assertEqual({}, res['Value']['Failed'])

  def test_isDirectory(self):
    """ Check if a path is a directory"""
    global mocked_xrootclient

    resource = XROOTStorage('storageName', self.parameterDict)

    statusMock = xrootStatusMock()
    statusMock.makeOk()

    statInfoMock = xrootStatInfoMock()
    statInfoMock.makeDir()

    updateStatMockReferences(statusMock, statInfoMock)

    # This test should be successful and True
    res = resource.isDirectory("A")
    self.assertEqual(True, res['OK'])
    self.assertEqual({}, res['Value']['Failed'])
    self.assertEqual({"A": True}, res['Value']['Successful'])

    statInfoMock.makeFile()

    # This test should be successful and False
    res = resource.isDirectory("A")
    self.assertEqual(True, res['OK'])
    self.assertEqual({}, res['Value']['Failed'])
    self.assertEqual({"A": False}, res['Value']['Successful'])

  def test_isFile(self):
    """ Check if a path is a File"""
    global mocked_xrootclient

    resource = XROOTStorage('storageName', self.parameterDict)

    statusMock = xrootStatusMock()
    statusMock.makeOk()

    statInfoMock = xrootStatInfoMock()
    statInfoMock.makeFile()

    updateStatMockReferences(statusMock, statInfoMock)

    # This test should be successful and True
    res = resource.isFile("A")
    self.assertEqual(True, res['OK'])
    self.assertEqual({}, res['Value']['Failed'])
    self.assertEqual({"A": True}, res['Value']['Successful'])

    # This test should be successful and True
    statusMock.makeError()
    res = resource.isFile("A")
    self.assertEqual(True, res['OK'])
    self.assertEqual({}, res['Value']['Successful'])
    self.assertEqual("A", res['Value']['Failed'].keys()[0])

    # This test should return S_ERROR
    statusMock.makeFatal()
    res = resource.isFile("A")
    self.assertEqual(False, res['OK'])

    statusMock.makeOk()
    statInfoMock.makeDir()

    # This test should be successful and False
    res = resource.isFile("A")
    self.assertEqual(True, res['OK'])
    self.assertEqual({}, res['Value']['Failed'])
    self.assertEqual({"A": False}, res['Value']['Successful'])

    # This test should be successful and True
    statusMock.makeError()
    res = resource.isFile("A")
    self.assertEqual(True, res['OK'])
    self.assertEqual({}, res['Value']['Successful'])
    self.assertEqual("A", res['Value']['Failed'].keys()[0])

    # This test should return S_ERROR
    statusMock.makeFatal()
    res = resource.isFile("A")
    self.assertEqual(False, res['OK'])

  def test_removeFile(self):
    ''' tests the output of removeFile
    '''
    global mocked_xrootclient

    resource = XROOTStorage('storageName', self.parameterDict)

    statusMock = xrootStatusMock()
    statusMock.makeOk()

    mocked_xrootclient.rm.return_value = statusMock, None

    # This test should be successful and True
    res = resource.removeFile("A")
    self.assertEqual(True, res['OK'])
    self.assertEqual({"A": True}, res['Value']['Successful'])
    self.assertEqual({}, res['Value']['Failed'])

    # This test should be successful and True (file was not there, so it is successfully deleted...)
    statusMock.makeError()
    statusMock.errno = 3011
    res = resource.removeFile("A")
    self.assertEqual(True, res['OK'])
    self.assertEqual({"A": True}, res['Value']['Successful'])
    self.assertEqual({}, res['Value']['Failed'])

    # This test should be in Failed
    statusMock.makeError()
    statusMock.errno = 0
    res = resource.removeFile("A")
    self.assertEqual(True, res['OK'])
    self.assertEqual({}, res['Value']['Successful'])
    self.assertEqual("A", res['Value']['Failed'].keys()[0])

    # This should return S_ERROR
    statusMock.makeFatal()
    res = resource.removeFile("A")
    self.assertEqual(False, res['OK'])

  def test_removeDirectory(self):
    ''' tests the output of removeDirectory
    '''
    global mocked_xrootclient
    resource = XROOTStorage('storageName', self.parameterDict)

    statusStatDirMock = xrootStatusMock()
    statusStatDirMock.makeOk()

    statInfoMock = xrootStatInfoMock()
    statInfoMock.makeDir()

    updateStatMockReferences(statusStatDirMock, statInfoMock)

    statDir1 = xrootStatInfoMock()
    statDir1.makeDir()
    statDir1.size = 1
    dir1 = xrootListEntryMock("dir1", "host", statDir1)

    statDir2 = xrootStatInfoMock()
    statDir2.makeDir()
    statDir2.size = 2
    dir2 = xrootListEntryMock("dir2", "host", statDir2)

    statFile1 = xrootStatInfoMock()
    statFile1.makeFile()
    statFile1.size = 4
    file1 = xrootListEntryMock("file1", "host", statFile1)

    statFile2 = xrootStatInfoMock()
    statFile2.makeFile()
    statFile2.size = 8
    file2 = xrootListEntryMock("file2", "host", statFile2)

    statFile3 = xrootStatInfoMock()
    statFile3.makeFile()
    statFile3.size = 16
    file3 = xrootListEntryMock("file3", "host", statFile3)

    directoryListMock1 = xrootDirectoryListMock("parent", [dir1, dir2, file1])
    directoryListMock2 = xrootDirectoryListMock("dir1", [file2])
    directoryListMock3 = xrootDirectoryListMock("dir2", [file3])

    statusMock = xrootStatusMock()
    statusMock.makeOk()
    mocked_xrootclient.rm.return_value = (statusMock, None)
    mocked_xrootclient.rmdir.return_value = (statusMock, None)

    # This test should remove file1 only
    setMockDirectory(directoryListMock1)

    res = resource.removeDirectory("A", recursive=False)
    self.assertEqual(True, res['OK'])
    self.assertEqual({"A": {"FilesRemoved": 1, "SizeRemoved": 4}}, res['Value']['Successful'])
    self.assertEqual({}, res['Value']['Failed'])

    mocked_xrootclient.dirlist.side_effect = [
        (statusStatDirMock,
         directoryListMock1),
        (statusStatDirMock,
         directoryListMock2),
        (statusStatDirMock,
         directoryListMock3)]

    mocked_xrootclient.stat.side_effect = None

    mockDirA = xrootStatInfoMock()
    mockDirA.makeDir()
    mockDirA.size = 3

    tmp_collect_sideeffs = [(statusStatDirMock, mockDirA)]
    for entry in directoryListMock1:
      tmp_tuple = (statusStatDirMock, entry.statinfo)
      tmp_collect_sideeffs.append(tmp_tuple)
    for entry in directoryListMock2:
      tmp_tuple = (statusStatDirMock, entry.statinfo)
      tmp_collect_sideeffs.append(tmp_tuple)
    for entry in directoryListMock3:
      tmp_tuple = (statusStatDirMock, entry.statinfo)
      tmp_collect_sideeffs.append(tmp_tuple)
    mocked_xrootclient.stat.side_effect = tmp_collect_sideeffs

    # This test should remove the 3 files
    res = resource.removeDirectory("A", recursive=True)
    self.assertEqual(True, res['OK'])
    self.assertEqual({"A": {"FilesRemoved": 3, "SizeRemoved": 28}}, res['Value']['Successful'])
    self.assertEqual({}, res['Value']['Failed'])

    # The rmdir command fails
    statusMock.makeError()
    mocked_xrootclient.dirlist.side_effect = None
    mocked_xrootclient.dirlist.return_value = statusMock, directoryListMock1
    res = resource.removeDirectory("A", recursive=False)
    self.assertEqual(True, res['OK'])
    self.assertEqual({}, res['Value']['Successful'])
    self.assertEqual({"A": {"FilesRemoved": 0, "SizeRemoved": 0}}, res['Value']['Failed'])

    # The rmdir command is fatal
    statusMock.makeFatal()
    mocked_xrootclient.dirlist.side_effect = [(statusStatDirMock, directoryListMock1)]
    res = resource.removeDirectory("A", recursive=False)
    self.assertEqual(True, res['OK'])
    self.assertEqual({}, res['Value']['Successful'])
    self.assertEqual({"A": {"FilesRemoved": 0, "SizeRemoved": 0}}, res['Value']['Failed'])

    # To get rid of rare bug that lets the makeFatal() survive into oter
    # tests, causing them to fail (notably listDir, if it is executed after
    # this test)
    statusMock.makeOk()
    mocked_xrootclient.rm.return_value = (None, None)
    mocked_xrootclient.rmdir.return_value = (None, None)
    mocked_xrootclient.dirlist.side_effect = [(None, None)]

  def test_getFile(self):
    """ Test the output of getFile"""
    global mocked_xrootclient
    global mocked_xrootd

    resource = XROOTStorage('storageName', self.parameterDict)

    statusMock = xrootStatusMock()
    statusMock.makeOk()

    mocked_xrootclient.copy.return_value = statusMock, None

    statusStatMock = xrootStatusMock()
    statusStatMock.makeOk()

    statInfoMock = xrootStatInfoMock()
    statInfoMock.makeFile()
    statInfoMock.size = -1

    updateStatMockReferences(statusStatMock, statInfoMock)

    # This test should be completely okay
    copymock = mock.Mock()
    copymock.run.return_value = (statusMock, None)
    mocked_xrootd.client.CopyProcess = mock.Mock(return_value=copymock)
    res = resource.getFile("a", "/tmp")
    self.assertEqual(True, res['OK'])
    self.assertEqual({"a": -1}, res['Value']['Successful'])
    self.assertEqual({}, res['Value']['Failed'])

    # Here the sizes should not match
    statInfoMock.size = 1000
    updateStatMockReferences(infoval=statInfoMock)
    res = resource.getFile("a", "/tmp")
    self.assertEqual(True, res['OK'])
    self.assertEqual({}, res['Value']['Successful'])
    self.assertEqual("a", res['Value']['Failed'].keys()[0])
    statInfoMock.size = -1

    # Here we should not be able to get the file from storage
    statusMock.makeError()
    updateStatMockReferences(statusMock)
    res = resource.getFile("a", "/tmp")
    self.assertEqual(True, res['OK'])
    self.assertEqual({}, res['Value']['Successful'])
    self.assertEqual("a", res['Value']['Failed'].keys()[0])

    # Fatal error in getting the file from storage
    updateStatMockReferences(statusMock)
    statusMock.makeFatal()
    res = resource.getFile("a", "/tmp")
    self.assertEqual(True, res['OK'])
    self.assertEqual({}, res['Value']['Successful'])
    self.assertEqual("a", res['Value']['Failed'].keys()[0])

  def test_getDirectory(self):
    ''' tests the output of getDirectory
    '''
    global mocked_xrootclient

    resource = XROOTStorage('storageName', self.parameterDict)

    statusStatDirMock = xrootStatusMock()
    statusStatDirMock.makeOk()

    statInfoMockDir = xrootStatInfoMock()
    statInfoMockDir.makeDir()

    statInfoMockFile = xrootStatInfoMock()
    statInfoMockFile.size = -1
    statInfoMockFile.makeFile()

    # Old comment, still true :(
    # This dirty thing forces us to know how many time api.stat is called and in what order...
    mocked_xrootclient.stat.side_effect = [
        (statusStatDirMock,
         statInfoMockDir),
        (statusStatDirMock,
         statInfoMockFile),
        (statusStatDirMock,
         statInfoMockDir),
        (statusStatDirMock,
         statInfoMockFile),
        (statusStatDirMock,
         statInfoMockDir),
        (statusStatDirMock,
         statInfoMockFile)]

    statDir1 = xrootStatInfoMock()
    statDir1.makeDir()
    statDir1.size = -1
    dir1 = xrootListEntryMock("dir1", "host", statDir1)

    statDir2 = xrootStatInfoMock()
    statDir2.makeDir()
    statDir2.size = -1
    dir2 = xrootListEntryMock("dir2", "host", statDir2)

    statFile1 = xrootStatInfoMock()
    statFile1.makeFile()
    statFile1.size = -1
    file1 = xrootListEntryMock("file1", "host", statFile1)

    statFile2 = xrootStatInfoMock()
    statFile2.makeFile()
    statFile2.size = -1
    file2 = xrootListEntryMock("file2", "host", statFile2)

    statFile3 = xrootStatInfoMock()
    statFile3.makeFile()
    statFile3.size = -1
    file3 = xrootListEntryMock("file3", "host", statFile3)

    directoryListMock1 = xrootDirectoryListMock("parent", [dir1, dir2, file1])
    directoryListMock2 = xrootDirectoryListMock("dir1", [file2])
    directoryListMock3 = xrootDirectoryListMock("dir1", [file3])

    statusMock = xrootStatusMock()
    statusMock.makeOk()

    mocked_xrootclient.copy.return_value = statusMock, None
    mocked_xrootclient.dirlist.side_effect = [
        (statusStatDirMock,
         directoryListMock1),
        (statusStatDirMock,
         directoryListMock2),
        (statusStatDirMock,
         directoryListMock3)]

    # This test should get the 3 files
    copymock = mock.Mock()
    copymock.run.return_value = (statusMock, None)
    mocked_xrootd.client.CopyProcess = mock.Mock(return_value=copymock)
    # Mock the os calls that access the filesystem and really create the directories locally.
    with mock.patch('os.makedirs',
                    new=MagicMock(return_value=True)), mock.patch('os.remove', new=MagicMock(return_value=True)):
      res = resource.getDirectory("A")
      self.assertEqual(True, res['OK'])
      self.assertEqual({"A": {"Files": 3, "Size": -3}}, res['Value']['Successful'])
      self.assertEqual({}, res['Value']['Failed'])

      # The copy command is just in error
      statusMock.makeError()
      mocked_xrootclient.dirlist.side_effect = [
          (statusStatDirMock,
           directoryListMock1),
          (statusStatDirMock,
           directoryListMock2),
          (statusStatDirMock,
           directoryListMock3)]
      mocked_xrootclient.stat.side_effect = [
          (statusStatDirMock,
           statInfoMockDir),
          (statusStatDirMock,
           statInfoMockFile),
          (statusStatDirMock,
           statInfoMockDir),
          (statusStatDirMock,
           statInfoMockFile),
          (statusStatDirMock,
           statInfoMockDir),
          (statusStatDirMock,
           statInfoMockFile)]

      res = resource.getDirectory("A")
      self.assertEqual(True, res['OK'])
      self.assertEqual({}, res['Value']['Successful'])
      self.assertEqual({"A": {"Files": 0, "Size": 0}}, res['Value']['Failed'])

      # The copy command is fatal
      statusMock.makeFatal()
      mocked_xrootclient.dirlist.side_effect = [
          (statusStatDirMock,
           directoryListMock1),
          (statusStatDirMock,
           directoryListMock2),
          (statusStatDirMock,
           directoryListMock3)]
      mocked_xrootclient.stat.side_effect = [
          (statusStatDirMock,
           statInfoMockDir),
          (statusStatDirMock,
           statInfoMockFile),
          (statusStatDirMock,
           statInfoMockDir),
          (statusStatDirMock,
           statInfoMockFile),
          (statusStatDirMock,
           statInfoMockDir),
          (statusStatDirMock,
           statInfoMockFile)]

      res = resource.getDirectory("A")
      self.assertEqual(True, res['OK'])
      self.assertEqual({}, res['Value']['Successful'])
      self.assertEqual({"A": {"Files": 0, "Size": 0}}, res['Value']['Failed'])

  @mock.patch('os.path.exists', new=MagicMock(return_value=True))
  @mock.patch('DIRAC.Resources.Storage.XROOTStorage.getSize', new=MagicMock(return_value=1))
  def test_putFile(self):
    """ Test the output of putFile"""

    global mocked_xrootclient

    resource = XROOTStorage('storageName', self.parameterDict)

    statusMock = xrootStatusMock()
    statusMock.makeOk()

    mocked_xrootclient.copy.return_value = statusMock, None

    statusMkDirMock = xrootStatusMock()
    statusMkDirMock.makeOk()

    mocked_xrootclient.mkdir.return_value = statusMkDirMock, None

    statusRmMock = xrootStatusMock()
    statusRmMock.makeOk()

    mocked_xrootclient.rm.return_value = statusRmMock, None

    statusStatMock = xrootStatusMock()
    statusStatMock.makeOk()

    statInfoMock = xrootStatInfoMock()
    statInfoMock.makeFile()
    statInfoMock.size = 1

    updateStatMockReferences(statusStatMock, statInfoMock)

    # This test should be completely okay
    copymock = mock.Mock()
    copymock.run.return_value = (statusMock, None)
    mocked_xrootd.client.CopyProcess = mock.Mock(return_value=copymock)
    res = resource.putFile({"remoteA": "localA"})
    self.assertEqual(True, res['OK'])
    self.assertEqual({"remoteA": 1}, res['Value']['Successful'])
    self.assertEqual({}, res['Value']['Failed'])

    # Here the sizes should not match
    statInfoMock.size = 1000
    res = resource.putFile({"remoteA": "localA"})
    self.assertEqual(True, res['OK'])
    self.assertEqual({}, res['Value']['Successful'])
    self.assertEqual("remoteA", res['Value']['Failed'].keys()[0])
    statInfoMock.size = 1

    # Here we should not be able to get the file from storage
    statusMock.makeError()
    res = resource.putFile({"remoteA": "localA"})
    self.assertEqual(True, res['OK'])
    self.assertEqual({}, res['Value']['Successful'])
    self.assertEqual("remoteA", res['Value']['Failed'].keys()[0])

    # Fatal error in getting the file from storage
    statusMock.makeFatal()
    res = resource.putFile({"remoteA": "localA"})
    self.assertEqual(True, res['OK'])
    self.assertEqual({}, res['Value']['Successful'])
    self.assertEqual("remoteA", res['Value']['Failed'].keys()[0])

    # Bad input
    res = resource.putFile("remoteA")
    self.assertEqual(False, res['OK'])

    # Error, but not 3011 when checking existance of file, and then successful anyway
    statusMock.makeOk()

    with mock.patch.object(XROOTStorage, '_XROOTStorage__singleExists',
                           return_value=S_OK(S_ERROR("error checking existance "))):
      res = resource.putFile({"remoteA": "localA"})
      self.assertEqual(True, res['OK'])
      self.assertEqual({'remoteA': 1}, res['Value']['Successful'])

  @mock.patch('os.path.isdir', new=MagicMock(side_effect=[True, True, True, False, True, True, False, False]))
  @mock.patch('os.listdir', new=MagicMock(side_effect=[["dir1", "dir2", "file1"], ["file2"], ["file3"]]))
  @mock.patch('os.path.exists', new=MagicMock(return_value=True))
  @mock.patch('DIRAC.Resources.Storage.XROOTStorage.getSize', new=MagicMock(return_value=1))
  def test_putDirectory(self):
    ''' tests the output of putDirectory
    '''
    global mocked_xrootclient

    # I again try to have 2 subdirs, and 1 file per subdir and 1 file a the root

    resource = XROOTStorage('storageName', self.parameterDict)

    statusCopyMock = xrootStatusMock()
    statusCopyMock.makeOk()

    mocked_xrootclient.copy.return_value = statusCopyMock, None

    statusMkDirMock = xrootStatusMock()
    statusMkDirMock.makeOk()

    mocked_xrootclient.mkdir.return_value = statusMkDirMock, None

    statusRmMock = xrootStatusMock()
    statusRmMock.makeOk()

    mocked_xrootclient.rm.return_value = statusRmMock, None

    statusStatMock = xrootStatusMock()
    statusStatMock.makeOk()
    statInfoMock = xrootStatInfoMock()
    statInfoMock.makeFile()
    statInfoMock.size = 1

    updateStatMockReferences(statusStatMock, statInfoMock)

    # This test should upload the 3 files
    copymock = mock.Mock()
    copymock.run.return_value = statusCopyMock, None
    mocked_xrootd.client.CopyProcess = mock.Mock(return_value=copymock)
    res = resource.putDirectory({"remoteA": "localA"})
    self.assertEqual(True, res['OK'])
    self.assertEqual({"remoteA": {"Files": 3, "Size": 3}}, res['Value']['Successful'])
    self.assertEqual({}, res['Value']['Failed'])

    # The copy command is just in error
    statusCopyMock.makeError()
    copymock.run.return_value = statusCopyMock, None
    mocked_xrootd.client.CopyProcess = mock.Mock(return_value=copymock)
    with mock.patch('os.path.isdir',
                    new=MagicMock(side_effect=[True,
                                               True,
                                               True,
                                               False,
                                               True,
                                               True,
                                               False,
                                               False])), mock.patch('os.listdir',
                                                                    new=MagicMock(side_effect=[("dir1",
                                                                                                "dir2",
                                                                                                "file1"),
                                                                                  ("file2", ), ("file3", )])):
      res = resource.putDirectory({"remoteA": "localA"})
    self.assertEqual(True, res['OK'])
    self.assertEqual({}, res['Value']['Successful'])
    self.assertEqual({"remoteA": {"Files": 0, "Size": 0}}, res['Value']['Failed'])

    # The copy command is fatal
    statusCopyMock.makeFatal()
    copymock.run.return_value = statusCopyMock, None
    mocked_xrootd.client.CopyProcess = mock.Mock(return_value=copymock)
    with mock.patch('os.path.isdir',
                    new=MagicMock(side_effect=[True,
                                               True,
                                               True,
                                               False,
                                               True,
                                               True,
                                               False,
                                               False])), mock.patch('os.listdir',
                                                                    new=MagicMock(side_effect=[("dir1",
                                                                                                "dir2",
                                                                                                "file1"),
                                                                                  ("file2", ), ("file3", )])):
      res = resource.putDirectory({"remoteA": "localA"})
    self.assertEqual(True, res['OK'])
    self.assertEqual({}, res['Value']['Successful'])
    self.assertEqual({"remoteA": {"Files": 0, "Size": 0}}, res['Value']['Failed'])

  def test_constructURLFromLFN(self):

    resource = XROOTStorage('storageName', self.parameterDict)

    resource.se = MagicMock()
    voName = "voName"
    resource.se.vo = voName
    testLFN = "/%s/path/to/filename" % voName

    # with spaceToken
    res = resource.constructURLFromLFN(testLFN)
    self.assertTrue(res['OK'])
    self.assertEqual("protocol://host//path%s?svcClass=%s" % (testLFN, self.parameterDict['SpaceToken']), res['Value'])

    # no spaceToken
    resource.protocolParameters['SpaceToken'] = ""
    res = resource.constructURLFromLFN(testLFN)
    self.assertTrue(res['OK'])
    self.assertEqual("protocol://host//path%s" % (testLFN, ), res['Value'])


def updateStatMockReferences(statval=None, infoval=None):
  """Updates the return value of stat to the new values provided. If a value is None,
     the old return value is used in that place.
     Should not be called with no arguments/ None None as arguments

    :param xrootStatusMock statval: Replaces the old value returned by stat, if set.
        If not set and infoval is set, the old value is used again.
    :param xrootStatInfoMock infoval: Replaces the old info value returned by stat, if set.
        If not set and statval is set, the old value is used again.
  """
  global mocked_xrootclient
  if statval is None or infoval is None:
    oldstat, oldinfo = mocked_xrootclient.stat.return_value
  if statval is not None and infoval is not None:
    newstat = statval
    newinfo = infoval
  elif statval is not None:
    # No infoval passed
    newstat = statval
    newinfo = oldinfo
  else:
    # No statval passed (case that both are None should never happen/makes no sense)
    newstat = oldstat
    newinfo = infoval
  mocked_xrootclient.stat.return_value = newstat, newinfo


def setMockDirectory(listmock, firstCall=True):
  """ Takes a xrootDirectoryListMock and sets the return value of dirlist as well
      as the return values (side effect) of stat calls.

      Does not work with nested directories because xrootDirectoryListMock
      contains no references to the subdirectories, so they are assumed empty

      :param xrootDirectoryListMock listmock: Values in this list
          are used to set the side_effect of stat and the return_value of dirlist
  """
  dirList = listmock.dirlist
  collect_side_effects = []
  global mocked_xrootclient
  if dirList is None:
    raise AttributeError('Something wrong with the calls, could not access dirlist')

  basestatval, _ = mocked_xrootclient.stat.return_value

  for entry in dirList:
    pair = (basestatval, entry.statinfo)
    collect_side_effects.append(pair)
  if firstCall:
    mocked_xrootclient.stat.side_effect = collect_side_effects
    mocked_xrootclient.dirlist.return_value = basestatval, listmock


if __name__ == '__main__':

  # Shut up the gLogger
  # Uncomment this line and comment the next 2 if you want debug

  suite = unittest.defaultTestLoader.loadTestsFromTestCase(XROOTStorage_Success)
  unittest.TextTestRunner(verbosity=2).run(suite)
