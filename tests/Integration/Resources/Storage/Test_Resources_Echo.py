"""
This integration tests will perform basic operations on Echo, depending on which protocols are available.
It creates a local hierarchy, and then tries to upload, download, remove, get metadata etc

.. warn::

  The storage element you test is supposed to be called 'RAL-ECHO'.
  (pylint does not play friendly with command line params...)


"""

# pylint: disable=invalid-name,wrong-import-position


from __future__ import print_function
from __future__ import absolute_import
from __future__ import division
import os
import tempfile
import shutil
import sys
import random

import pytest

from DIRAC.Core.Base import Script


# ugly hack. We remove all the pytest options before the script name
# in order to make parseCommandLine happy
# Though we can't pass options to that script
SCRIPT_NAME = os.path.basename(__file__)
for pos, elem in enumerate(sys.argv):
  if SCRIPT_NAME in elem:
    break
sys.argv = sys.argv[pos:]


Script.parseCommandLine()


from DIRAC import gLogger
from DIRAC.Core.Utilities.Adler import fileAdler
from DIRAC.Core.Utilities.File import getSize
from DIRAC.Resources.Storage.StorageElement import StorageElement
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOForGroup


# #### GLOBAL VARIABLES: ################

# Name of the storage element that has to be tested
gLogger.setLevel('DEBUG')

STORAGE_NAME = 'RAL-ECHO'
# Size in bytes of the file we want to produce
FILE_SIZE = 5 * 1024  # 5kB
# base path on the storage where the test files/folders will be created
DESTINATION_PATH = ''
# plugins that will be used

# These variables are defined by the setup fixture
local_path = putDir = createDir = putFile = isFile = listDir = getFile = rmDir = None
getDir = removeFile = filesInFolderAandB = fileAdlers = fileSizes = None
se = None

try:
  res = getProxyInfo()
  if not res['OK']:
    gLogger.error("Failed to get client proxy information.", res['Message'])
    sys.exit(2)
  proxyInfo = res['Value']
  username = proxyInfo['username']
  vo = ''
  if 'group' in proxyInfo:
    vo = getVOForGroup(proxyInfo['group'])

  DESTINATION_PATH = '/%s/user/%s/%s/gfaltests' % (vo, username[0], username)

except Exception as e:  # pylint: disable=broad-except
  print(repr(e))
  sys.exit(2)


# local path containing test files. There should be a folder called Workflow containing
# (the files can be simple textfiles)
# FolderA
# -FolderAA
# --FileAA
# -FileA
# FolderB
# -FileB
# File1
# File2
# File3


def _mul(txt):
  """ Multiply the input text enough time so that we
      reach the expected file size
  """
  return txt * (max(1, FILE_SIZE / len(txt)))


def clearDirectory(se, local_path, target_path):
  """ Removing target directory """
  print("==================================================")
  print("==== Removing the older Directory ================")
  filesToRemove = []
  for root, _dirs, files in os.walk(local_path):
    for fn in files:
      filesToRemove.append(
          os.path.join(
              target_path,
              root.replace(
                  local_path,
                  '').strip('/'),
              fn))

  print("CHRIS WILL REMOVE %s" % filesToRemove)
  res = se.removeFile(filesToRemove)
  if not res['OK']:
    print("basicTest.clearDirectory: Workflow folder maybe not empty")
  print("==================================================")


@pytest.fixture(scope="module")
def setuptest(request):
  global local_path, putDir, createDir, putFile, isFile, listDir,\
      getDir, getFile, rmDir, removeFile, se, filesInFolderAandB, fileAdlers, fileSizes
  local_path = tempfile.mkdtemp()

  # create the local structure
  workPath = os.path.join(local_path, 'Workflow')
  os.mkdir(workPath)

  os.mkdir(os.path.join(workPath, 'FolderA'))
  with open(os.path.join(workPath, 'FolderA', 'FileA'), 'w') as f:
    f.write(_mul('FileA'))

  os.mkdir(os.path.join(workPath, 'FolderA', 'FolderAA'))
  with open(os.path.join(workPath, 'FolderA', 'FolderAA', 'FileAA'), 'w') as f:
    f.write(_mul('FileAA'))

  os.mkdir(os.path.join(workPath, 'FolderB'))
  with open(os.path.join(workPath, 'FolderB', 'FileB'), 'w') as f:
    f.write(_mul('FileB'))

  for fn in ["File1", "File2", "File3"]:
    with open(os.path.join(workPath, fn), 'w') as f:
      f.write(_mul(fn))

  se = StorageElement(STORAGE_NAME)

  putDir = {os.path.join(DESTINATION_PATH,
                         'Workflow/FolderA'): os.path.join(local_path,
                                                           'Workflow/FolderA'),
            os.path.join(DESTINATION_PATH,
                         'Workflow/FolderB'): os.path.join(local_path,
                                                           'Workflow/FolderB')}

  createDir = [os.path.join(DESTINATION_PATH, 'Workflow/FolderA/FolderAA'),
               os.path.join(DESTINATION_PATH, 'Workflow/FolderA/FolderABA'),
               os.path.join(DESTINATION_PATH, 'Workflow/FolderA/FolderAAB')
               ]

  putFile = {os.path.join(DESTINATION_PATH,
                          'Workflow/FolderA/File1'): os.path.join(local_path,
                                                                  'Workflow/File1'),
             os.path.join(DESTINATION_PATH,
                          'Workflow/FolderAA/File1'): os.path.join(local_path,
                                                                   'Workflow/File1'),
             os.path.join(DESTINATION_PATH,
                          'Workflow/FolderBB/File2'): os.path.join(local_path,
                                                                   'Workflow/File2'),
             os.path.join(DESTINATION_PATH,
                          'Workflow/FolderB/File2'): os.path.join(local_path,
                                                                  'Workflow/File2'),
             os.path.join(DESTINATION_PATH,
                          'Workflow/File3'): os.path.join(local_path,
                                                          'Workflow/File3')}

  isFile = list(putFile)

  listDir = [os.path.join(DESTINATION_PATH, 'Workflow'),
             os.path.join(DESTINATION_PATH, 'Workflow/FolderA'),
             os.path.join(DESTINATION_PATH, 'Workflow/FolderB')
             ]

  getDir = [os.path.join(DESTINATION_PATH, 'Workflow/FolderA'),
            os.path.join(DESTINATION_PATH, 'Workflow/FolderB')
            ]

  removeFile = [os.path.join(DESTINATION_PATH, 'Workflow/FolderA/File1')]
  rmdir = [os.path.join(DESTINATION_PATH, 'Workflow')]

  # This list is used to check for existance of files
  # after uploading the directory: they should NOT exist.
  # Uploading a directory does not work.
  filesInFolderAandB = []
  for dirName in ('Workflow/FolderA', 'Workflow/FolderB'):
    for root, _dirs, files in os.walk(os.path.join(local_path, dirName)):
      for fn in files:
        filesInFolderAandB.append(
            os.path.join(
                DESTINATION_PATH,
                root.replace(
                    local_path,
                    '').strip('/'),
                fn))
  filesInFolderAandB = dict.fromkeys(filesInFolderAandB, False)

  fileAdlers = {}
  fileSizes = {}

  for lfn, localFn in putFile.items():
    fileAdlers[lfn] = fileAdler(localFn)
    fileSizes[lfn] = getSize(localFn)

  clearDirectory(se, local_path, DESTINATION_PATH)

  def teardown():
    print("Cleaning local test")
    shutil.rmtree(local_path)
    clearDirectory(se, local_path, DESTINATION_PATH)

  request.addfinalizer(teardown)
  return local_path, random.randint(0, 100)  # provide the fixture value


# def test_hello(setuptest):
#   print 'in test hello %s %s'%(setuptest, putFile)
#
# def test_hello2(setuptest):
#   print 'in test hello2 %s %s'%(setuptest)

@pytest.mark.order1
def test_uploadDirectory_shouldFail(setuptest):
  """ uploading directories is not possible with Echo"""
  res = se.putDirectory(putDir)
  assert res['OK']
  assert set(res['Value']['Failed']) == set(putDir)

  print("CHRIS uploadDir %s" % res)
  # Need to sleep for echo to update ?
  # time.sleep(1)

  res = se.exists(filesInFolderAandB)
  assert res['OK'], res

  for fn in filesInFolderAandB:
    assert fn in res['Value']['Successful']
    assert res['Value']['Successful'][fn] is False


@pytest.mark.order2
def test_listDirectory_shouldFail(setuptest):
  """ Listing directory should fail """
  res = se.listDirectory(listDir)
  for dn in listDir:
    assert dn in res['Value']['Failed'], res


@pytest.mark.order3
def test_createDirectory(setuptest):
  """ Echo is nice enough to tell us that it is a success..."""
  res = se.createDirectory(createDir)
  for dn in createDir:
    assert dn in res['Value']['Successful'], res


@pytest.mark.order4
def test_putFile(setuptest):
  """ Copy a file """
  res = se.putFile(putFile)
  assert res['OK'], res
  # time.sleep(0.2)


@pytest.mark.order5
def test_isFile(setuptest):
  """ Test whether an LFN is a file """
  res = se.isFile(isFile)
  assert res['OK'], res
  for lfn in isFile:
    assert res['Value']['Successful'][lfn], res


@pytest.mark.order6
def test_getFileMetadata(setuptest):
  """ Get the metadata of previously uploaded files """
  res = se.getFileMetadata(isFile)
  assert res['OK'], res
  res = res['Value']['Successful']
  assert sorted(res) == sorted(isFile)
  assert any(path in resKey for path in isFile for resKey in res.keys())

  # Checking that the checksums and sizes are correct
  for lfn in isFile:
    assert res[lfn]['Checksum'] == fileAdlers[lfn]
    assert res[lfn]['Size'] == fileSizes[lfn]


@pytest.mark.order7
def test_getDirectory_shouldFail(setuptest):
  """Get directory cannot work on Echo"""

  res = se.getDirectory(getDir, os.path.join(local_path, 'getDir'))
  for dn in getDir:
    assert dn in res['Value']['Failed'], res


@pytest.mark.order8
def test_removeFile(setuptest):
  """ Remove files """
  res = se.removeFile(removeFile)
  assert res['OK'], res
  res = se.exists(removeFile)
  assert res['OK'], res
  assert res['Value']['Successful'][removeFile[0]] is False


@pytest.mark.order9
def test_removeNonExistingFile(setuptest):
  """ remove non existing file """
  res = se.removeFile(removeFile)
  assert res['OK'], res
  res = se.exists(removeFile)
  assert res['OK'], res
  for fn in removeFile:
    assert res['Value']['Successful'][fn] is False
