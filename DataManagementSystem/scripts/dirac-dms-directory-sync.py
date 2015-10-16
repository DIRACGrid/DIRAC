#!/usr/bin/env python
########################################################################
# :file:    dirac-dms-directory-sync
# :author:  Marko Petric
########################################################################
"""
Provides basic rsync functionality for DIRAC

Syncs the source destination folder recursivly into the target destination

If option --sync is used contend that is not in the source directory but is
only in the target directory will be deleted.
"""

__RCSID__ = "$Id$"

import os
import sys
import DIRAC
from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s Source Destination' % Script.scriptName,
                                     ' ',
                                     ' e.g.: Download',
                                     '   %s LFN Path' % Script.scriptName,
                                     '  or Upload',
                                     '   %s Path LFN SE' % Script.scriptName,
                                     'Arguments:',
                                     '  LFN:      Logical File Name (Path to directory)',
                                     '  Path:     Local path to the file (Path to directory)',
                                     '  SE:       DIRAC Storage Element',
                                     '  --sync    Exact sync of source to target (some files might be deleted)']
                                 )
                      )

Script.registerSwitch( "D" , "sync" , "Make target directory identical to source" )
Script.parseCommandLine( ignoreErrors = False )

args = Script.getPositionalArgs()
if len( args ) < 1 or len( args ) > 3:
  Script.showHelp()

sync = False
for switch in Script.getUnprocessedSwitches():
  if switch[0].lower() == "s" or switch[0].lower() == "sync":
    sync = True


from DIRAC import S_OK, S_ERROR
from DIRAC import gConfig, gLogger
from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
from DIRAC.Core.Utilities.List import sortList, breakListIntoChunks
from DIRAC.DataManagementSystem.Client.DataManager import DataManager
from DIRAC.Resources.Storage.StorageElement import StorageElement
from DIRAC.Core.Utilities.ReturnValues import returnSingleResult

def getSetOfLocalDirectoriesAndFiles( path ):
  """Return a set of all directories and subdirectories and a set of
  files contained therein for a given local path
  """

  fullPath = os.path.abspath(path)

  if not os.path.isdir(fullPath):
    return S_ERROR("The path: " + fullPath + " does not exist!")

  directories = set()
  files = set()

  for dirname, dirnames, filenames in os.walk(path):
  # add path to all subdirectories first.
    for subdirname in dirnames:
      fullSubdirname = os.path.join(dirname,subdirname)
      fullSubdirname = os.path.abspath(fullSubdirname)
      fullSubdirname = fullSubdirname.replace(fullPath,'').lstrip('/')
      directories.add(fullSubdirname)
  # add path to all filenames.
    for filename in filenames:
      fullFilename = os.path.join(dirname,filename)
      fullFilename = os.path.abspath(fullFilename)
      fullFilename = fullFilename.replace(fullPath,'').lstrip('/')
      fileSize = os.path.getsize(fullPath + "/" +  fullFilename)
      if fileSize > 0:
        files.add((fullFilename,long(fileSize)))

  tree = {}
  tree["Directories"]=directories
  tree["Files"]=files

  return S_OK(tree)

def getSetOfRemoteSubDirectoriesAndFiles(path,fc,directories,files):
  """
  Recursively traverses all the subdirectories of a directory and returns a set of directories and files
  """
  result = fc.listDirectory(path)
  if result['OK']:
    if result['Value']['Successful']:
      for entry in result['Value']['Successful'][path]['Files']:
        size = result['Value']['Successful'][path]['Files'][entry]['MetaData']['Size']
        files.add((entry,size))
      for entry in result['Value']['Successful'][path]['SubDirs']:
        directories.add(entry)
        res = getSetOfRemoteSubDirectoriesAndFiles(entry,fc,directories,files)
        if not res['OK']:
          return S_ERROR('Error: ' + res['Message'])
      return S_OK()
    else:
      return S_ERROR("Error:" + result['Message'])
  else:
    return S_ERROR("Error:" + result['Message'])

def getSetOfRemoteDirectoriesAndFiles(fc, path):
  """
    Return a set of all directories and subdirectories and the therein contained files for a given LFN
  """
  directories = set()
  files = set()

  res = getSetOfRemoteSubDirectoriesAndFiles(path,fc,directories,files)
  if not res['OK']:
    return S_ERROR('Could not list remote directory: ' + res['Message'])

  return_directories = set()
  return_files = set()

  for myfile in files:
    return_files.add((myfile[0].replace(path,'').lstrip('/'),myfile[1]))

  for mydirectory in directories:
    return_directories.add(mydirectory.replace(path,'').lstrip('/'))

  tree = {}
  tree["Directories"]=return_directories
  tree["Files"]=return_files

  return S_OK(tree)

def isInFileCatalog(fc, path ):
  """
  Check if the file is in the File Catalog
  """

  result = fc.listDirectory(path)
  if result['OK']:
    if result['Value']['Successful']:
      return S_OK()
    else:
      return S_ERROR()
  else:
    return S_ERROR()

def getContentToSync(upload, fc, source_dir, dest_dir):

  if upload:
    res = getSetOfRemoteDirectoriesAndFiles(fc, dest_dir)
    if not res['OK']:
      return S_ERROR(res['Message'])
    to_dirs = res['Value']['Directories']
    to_files = res['Value']['Files']

    res = getSetOfLocalDirectoriesAndFiles(source_dir)
    if not res['OK']:
      return S_ERROR(res['Message'])
    from_dirs = res['Value']['Directories']
    from_files = res['Value']['Files']

  else:
    res = getSetOfLocalDirectoriesAndFiles(dest_dir)
    if not res['OK']:
      return S_ERROR(res['Message'])
    to_dirs = res['Value']['Directories']
    to_files = res['Value']['Files']

    res = getSetOfRemoteDirectoriesAndFiles(fc, source_dir)
    if not res['OK']:
      return S_ERROR(res['Message'])
    from_dirs = res['Value']['Directories']
    from_files = res['Value']['Files']

  #Create list of directories to delete
  dirs_delete = list(to_dirs - from_dirs)
  #Sort the list by depth of directory tree
  dirs_delete.sort(key = lambda s: -s.count('/'))
  #Create list of directories to create
  dirs_create = list(from_dirs - to_dirs)
  #Sort the list by depth of directory tree
  dirs_create.sort(key = lambda s: s.count('/'))

  #Flatten the list of pairs (filename, size) to list of filename
  files_delete = [pair[0] for pair in list(to_files - from_files)]
  files_create = [pair[0] for pair in list(from_files - to_files)]

  create = {}
  create["Directories"] = dirs_create
  create["Files"] = files_create

  delete = {}
  delete["Directories"] = dirs_delete
  delete["Files"] = files_delete

  tree = {}
  tree["Create"]=create
  tree["Delete"]=delete

  return S_OK(tree)


def removeRemoteFiles(dm,lfns):
  """
  Remove file from the catalog
  """
  for lfnList in breakListIntoChunks( lfns, 100 ):
    res = dm.removeFile( lfnList )
    if not res['OK']:
      return S_ERROR( "Failed to remove files:" + lfnList + res['Message'] )
    else:
      return S_OK()


def uploadLocalFile(dm, lfn, localfile, storage):
  """
    Upload a local file to a storage element
  """
  res = dm.putAndRegister( lfn, localfile, storage, None )
  if not res['OK']:
    return S_ERROR( 'Error: failed to upload %s to %s' % ( lfn, storage ) )
  else:
    return S_OK( 'Successfully uploaded file to %s' % storage )

def downloadRemoteFile(dm, lfn, destination):
  """
  Download a file from the system
  """
  res = dm.getFile( lfn, destination )
  if not res['OK']:
    return S_ERROR( 'Error: failed to download %s ' % lfn )
  else:
    return S_OK( 'Successfully uploaded file %s' % lfn )

def removeStorageDirectoryFromSE( directory, storageElement ):
  """
  Delete directory on selected storage element
  """

  se = StorageElement( storageElement, False )
  res = returnSingleResult( se.exists( directory ) )

  if not res['OK']:
    return S_ERROR( "Failed to obtain existence of directory" + res['Message'] )

  exists = res['Value']
  if not exists:
    return S_OK( "The directory %s does not exist at %s " % ( directory, storageElement ) )

  res = returnSingleResult( se.removeDirectory( directory, recursive = True ) )
  if not res['OK']:
    return S_ERROR( "Failed to remove storage directory" + res['Message'] )

  return S_OK()

def removeRemoteDirectory(fc,lfn):
  """
  Remove file from the catalog
  """
  storageElements = gConfig.getValue( 'Resources/StorageElementGroups/SE_Cleaning_List', [] )

  for storageElement in sorted( storageElements ):
    res = removeStorageDirectoryFromSE( lfn, storageElement )
    if not res['OK']:
      return S_ERROR( "Failed to clean storage directory at all SE:" + res['Message'] )
  res = returnSingleResult( fc.removeDirectory( lfn, recursive = True ) )
  if not res['OK']:
    return S_ERROR( "Failed to clean storage directory at all SE:" + res['Message'] )

  return S_OK("Successfully removed directory")


def createRemoteDirectory(fc,newdir):
  """
  Create directory in file catalog
  """
  result = fc.createDirectory(newdir)
  if result['OK']:
    if result['Value']['Successful'] and result['Value']['Successful'].has_key(newdir):
      return S_OK("Successfully created directory:" + newdir)
    elif result['Value']['Failed'] and result['Value']['Failed'].has_key(newdir):
      return S_ERROR('Failed to create directory: ' + result['Value']['Failed'][newdir])
  else:
    return S_ERROR('Failed to create directory:' + result['Message'])

def createLocalDirectory(directory):
  """
  Create local directory
  """
  try:
    os.makedirs(directory)
  except OSError as e:
    return S_ERROR('Directory creation failed: ' + e.strerror)

  if not os.path.exists(directory):
    return S_ERROR('Directory creation failed')
  return S_OK('Created directory successfully')

def removeLocalFile(path):
  """
  Remove local file
  """
  try:
    os.remove(path)
  except OSError as e:
    return S_ERROR('Directory creation failed:' + e.strerror)

  if os.path.isfile(path):
    return S_ERROR('File deleting failed')
  return S_OK('Removed file successfully')

def removeLocaDirectory(path):
  """
  Remove local directory
  """
  try:
    os.rmdir(path)
  except OSError as e:
    return S_ERROR('Deleting directory failed: ' + e.strerror)

  if os.path.isdir(path):
    return S_ERROR('Directory deleting failed')
  return S_OK('Removed directory successfully')

def doUpload(fc, dm, result, source_dir, dest_dir, storage, delete):
  """
  Wrapper for uploading files
  """
  if delete:
    lfns = [dest_dir+"/"+filename for filename in result['Value']['Delete']['Files']]
    if len(lfns)>0:
      gLogger.notice("Deleting "+ ', '.join(lfns))
      res = removeRemoteFiles(dm,lfns)
      if not res['OK']:
        return S_ERROR('Failed to remove files: ' + lfns + res['Message'])
      gLogger.notice("[DONE]")

    for directoryname in result['Value']['Delete']['Directories']:
      gLogger.notice("Deleting "+ directoryname)
      res = removeRemoteDirectory(fc, dest_dir + "/" + directoryname)
      if not res['OK']:
        return S_ERROR('Failed to remove directory: '+ directoryname + res['Message'])
      gLogger.notice("[DONE]")


  for directoryname in result['Value']['Create']['Directories']:
    gLogger.notice("Creating " + directoryname)
    res = createRemoteDirectory(fc, dest_dir+"/"+ directoryname)
    if not res['OK']:
      return S_ERROR('Directory creation failed: ' + res['Message'])
    gLogger.notice("[DONE]")

  for filename in result['Value']['Create']['Files']:
    gLogger.notice("Uploading " + filename)
    res = uploadLocalFile(dm, dest_dir+"/"+filename, source_dir+"/"+filename, storage)
    if not res['OK']:
      return S_ERROR('Upload of file: ' + filename + ' failed ' + res['Message'])
    gLogger.notice("[DONE]")

  return S_OK('Upload finished successfully')

def doDownload(dm, result, source_dir, dest_dir, delete):
  """
  Wrapper for downloading files
  """
  if delete:
    for filename in result['Value']['Delete']['Files']:
      gLogger.notice("Deleting "+ filename)
      res = removeLocalFile(dest_dir+"/"+ filename)
      if not res['OK']:
        return S_ERROR('Deleting of file: ' + filename + ' failed ' + res['Message'])
      gLogger.notice("[DONE]")

    for directoryname in result['Value']['Delete']['Directories']:
      gLogger.notice("Deleting "+ directoryname)
      res = removeLocaDirectory( dest_dir + "/" + directoryname )
      if not res['OK']:
        return S_ERROR('Deleting of directory: ' + directoryname + ' failed ' + res['Message'])
      gLogger.notice("[DONE]")

  for directoryname in result['Value']['Create']['Directories']:
    gLogger.notice("Creating " + directoryname)
    res = createLocalDirectory( dest_dir+"/"+ directoryname )
    if not res['OK']:
      return S_ERROR('Creation of directory: ' + directoryname + ' failed ' + res['Message'])
    gLogger.notice("[DONE]")

  for filename in result['Value']['Create']['Files']:
    gLogger.notice("Downloading " + filename)
    res = downloadRemoteFile(dm, source_dir + "/" + filename, dest_dir + ("/" + filename).rsplit("/", 1)[0])
    if not res['OK']:
      return S_ERROR('Download of file: ' + filename + ' failed ' + res['Message'])
    gLogger.notice("[DONE]")

  return S_OK('Upload finished successfully')

def syncDestinations(upload, source_dir, dest_dir, storage, delete ):
  """
  Top level wrapper to execute functions
  """

  fc = FileCatalog()
  dm = DataManager()

  result = getContentToSync(upload,fc,source_dir,dest_dir)
  if not result['OK']:
    return S_ERROR(result['Message'])

  if upload:
    res = doUpload(fc, dm, result, source_dir, dest_dir, storage, delete)
    if not res['OK']:
      return S_ERROR('Upload failed: ' + res['Message'])
  else:
    res = doDownload(dm, result, source_dir, dest_dir, delete)
    if not res['OK']:
      return S_ERROR('Download failed: ' + res['Message'])

  return S_OK('Mirroring successfully finished')

def run( parameters , delete ):
  """
  The main user interface
  """

  source_dir = parameters[0]
  dest_dir = parameters[1]
  upload = False
  storage = None

  if len( parameters ) == 3:
    storage = parameters[2]
    source_dir = os.path.abspath(source_dir)
    dest_dir = dest_dir.rstrip('/')
    upload = True
    if not os.path.isdir(source_dir):
      gLogger.fatal("Source directory does not exist")
      sys.exit(1)

  if len (parameters ) == 2:
    dest_dir = os.path.abspath(dest_dir)
    source_dir = source_dir.rstrip('/')
    if not os.path.isdir(dest_dir):
      gLogger.fatal("Destination directory does not exist")
      sys.exit(1)

  res = syncDestinations( upload, source_dir, dest_dir, storage, delete )
  if not res['OK']:
    return S_ERROR(res['Message'])

  return S_OK("Successfully mirrored " + source_dir + " into " + dest_dir)

if __name__ == "__main__":
  message = run( args , sync )
  gLogger.notice(message['Value'])
