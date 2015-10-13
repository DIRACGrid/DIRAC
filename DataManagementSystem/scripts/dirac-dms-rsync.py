#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-dms-rsync
# Author :  Marko Petric
########################################################################
"""
  Porvides basic rsync funcionality for DIRAC
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
                                     '  Path:     Local path to the file (Path to direcotry)',
                                     '  SE:       DIRAC Storage Element' ]
                                 )
                      )
Script.parseCommandLine( ignoreErrors = False )


from DIRAC import S_OK, S_ERROR
from DIRAC import gConfig
from DIRAC.Resources.Catalog.FileCatalogFactory import FileCatalogFactory
from DIRAC.Core.Utilities.List import sortList, breakListIntoChunks
from DIRAC.DataManagementSystem.Client.DataManager import DataManager
from DIRAC.Resources.Storage.StorageElement import StorageElement
from DIRAC.Core.Utilities.ReturnValues import returnSingleResult

def getSetOfLocalDirectoriesAndFiles( path ):
  """
  Return a set of all directories and subdirectories and a set of files contained therein for a given local path
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
      files.add((fullFilename,long(os.path.getsize(fullPath + "/" +  fullFilename))))

  tree = {}
  tree["Directories"]=directories
  tree["Files"]=files

  return S_OK(tree)

def getFileCatalog():
  """
    Returns the DIRAC file catalog
  """
  fcType = gConfig.getValue("/LocalSite/FileCatalog","")
  
  res = gConfig.getSections("/Resources/FileCatalogs",listOrdered = True)
  if not res['OK']:
    return S_ERROR(res['Message'])
    
  if not fcType:
    if res['OK']:
      fcType = res['Value'][0]
  
  if not fcType:
    return S_ERROR("No file catalog given and defaults could not be obtained")
  
  result = FileCatalogFactory().createCatalog(fcType)
  if not result['OK']:
    return S_ERROR(result['Message'])
    
  fc = result['Value']
  
  return S_OK(fc)


def getSetOfRemoteSubDirectoriesAndFiles(path,fc,directories,files):
  """
    Recusivly traverses all the subdirectories of a directory and returns a set of directories and files
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
          return S_ERROR('Error: ' + res['Massage'])
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
    return S_ERROR('Could not list remote directory: ' + res['Massage'])

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
    to_files =  res['Value']['Files']
    
    res = getSetOfLocalDirectoriesAndFiles(source_dir)
    if not res['OK']:
      return S_ERROR(res['Message'])
    from_dirs = res['Value']['Directories']
    from_files =  res['Value']['Files']
    
  else:
    res = getSetOfLocalDirectoriesAndFiles(dest_dir)
    if not res['OK']:
      return S_ERROR(res['Message'])
    to_dirs = res['Value']['Directories']
    to_files =  res['Value']['Files']
    
    res = getSetOfRemoteDirectoriesAndFiles(fc, source_dir)
    if not res['OK']:
      return S_ERROR(res['Message'])
    from_dirs = res['Value']['Directories']
    from_files =  res['Value']['Files']
    
  
  dirs_delete = list(to_dirs - from_dirs)
  dirs_delete.sort(key = lambda s: -s.count('/'))
  dirs_create = list(from_dirs - to_dirs)
  dirs_create.sort(key = lambda s: s.count('/'))

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
      return S_ERROR( "Failed to remove data", res['Message'] )
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
    delete directory on selected storage element
  """
  
  se = StorageElement( storageElement, False )
  res = returnSingleResult( se.exists( directory ) )

  if not res['OK']:
    return S_ERROR( "Failed to obtain existance of directory", res['Message'] )

  exists = res['Value']
  if not exists:
    return S_OK( "The directory %s does not exist at %s " % ( directory, storageElement ) )

  res = returnSingleResult( se.removeDirectory( directory, recursive = True ) )
  if not res['OK']:
    return S_ERROR( "Failed to remove storage directory", res['Message'] )
  
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
  
  return S_OK("Sucefully removed directory")


def createRemoteDirectory(fc,newdir):
  """
    Create directory in file catalog
  """
  result = fc.createDirectory(newdir)
  if result['OK']:
    if result['Value']['Successful']:
      if result['Value']['Successful'].has_key(newdir):
        return S_OK("Successfully created directory:" + newdir)
    elif result['Value']['Failed']:
      if result['Value']['Failed'].has_key(newdir):
        return S_ERROR('Failed to create directory:',result['Value']['Failed'][newdir])
  else:
    return S_ERROR('Failed to create directory:' + result['Message'])

def createLocalDirectory(directory):
  """
  Create local directory
  """
  os.makedirs(directory)
  if not os.path.exists(directory):
    return S_ERROR('Directory creation failed')
  return S_OK('Created directory sucefully')

def removeLocalFile(path):
  """
  Remove local file
  """
  os.remove(path)
  if os.path.isfile(path):
    return S_ERROR('File deleting failed')
  return S_OK('Removed file sucefully')

def syncDestinations(upload, source_dir, dest_dir, storage="CERN-DST-EOS"):
  """
  Top level wrapper to execute functions
  """
  
  result = getFileCatalog()
  if not result['OK']:
    return S_ERROR(result['Message'])
  fc = result['Value']
    
  dm = DataManager()
  
  result = getContentToSync(upload,fc,source_dir,dest_dir)
  if not result['OK']:
    return S_ERROR(result['Message'])
  
  if upload:
    
    lfns =  [dest_dir+"/"+_file for _file in result['Value']['Delete']['Files']]
    res = removeRemoteFiles(dm,lfns)
    if not res['OK']:
      return S_ERROR('Failed to remove files: ' + lfns + res['Message'])
    
    for _directory in result['Value']['Delete']['Directories']:
      res = removeRemoteDirectory(fc, dest_dir + "/" + _directory)
      if not res['OK']:
        return S_ERROR('Failed to remove directory: '+ _directory + res['Message'])
        
    for _directory in result['Value']['Create']['Directories']:
      res = createRemoteDirectory(fc, dest_dir+"/"+ _directory)
      if not res['OK']:
        return S_ERROR('Directory creation failed: ' + res['Message'])
        
    for _file in result['Value']['Create']['Files']:
      res = uploadLocalFile(dm, dest_dir+"/"+_file, source_dir+"/"+_file, storage)
      if not res['OK']:
        return S_ERROR('Upload of file: ' + _file + ' failed ' + res['Message'])
      
  else:
    for _file in result['Value']['Delete']['Files']:
      print dest_dir+"/"+ _file
    for _directory in result['Value']['Delete']['Directories']:
      print dest_dir + "/" + _directory
    for _file in result['Value']['Create']['Files']:
      print dest_dir+"/"+ _file
    for _directory in result['Value']['Create']['Directories']:
      print dest_dir + "/" + _directory
  
def run():
  """doc"""
#  res = getFileCatalog()
#  fc = res['Value']
  #dm = DataManager()
  syncDestinations(True, '/afs/cern.ch/user/p/petric/grid/DIRAC/DataManagementSystem/scripts', '/ilc/user/p/petric')
  
if __name__ == "__main__":
  run()
 



  

