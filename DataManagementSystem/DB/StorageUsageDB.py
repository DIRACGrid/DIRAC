""" StorageUsageDB class is a front-end to the Storage Usage Database.
"""
import re, os, sys,threading
import time, datetime
from types import *

from DIRAC import gConfig,gLogger,S_OK, S_ERROR
from DIRAC.Core.Base.DB import DB

#############################################################################

class StorageUsageDB(DB):

  def __init__( self, maxQueueSize=10 ):
    """ Standard Constructor
    """
    DB.__init__(self,'StorageUsageDB','DataManagement/StorageUsageDB',maxQueueSize)
    self.getIdLock = threading.Lock()

  #############################################################################
  #
  # Methods for manipulating the multiple tables
  #

  def insertDirectory(self,directory,directoryFiles,directorySize):
    """ Insert the directory into the Directory table and adds the parameters to the DirectoryParameters table
    """
    res = self.__getDirectoryID(directory)
    if not res['OK']:
      return res
    elif res['Value']:
      # The directory already exists
      return S_OK()
    else:
      res = self.__insertDirectory(directory, directoryFiles, directorySize)
      if not res['OK']:
        errStr = res['Message']
        res = self.removeDirectory(directory)
        return S_ERROR(errStr)
      else:
        directoryID = res['Value']
        for parameter in directory.split('/'):
          if parameter:
            res = self.__insertDirectoryParam(directoryID, parameter)
            if not res['OK']:
              errStr = res['Message']
              res = self.removeDirectory(directory)
              return S_ERROR(errStr)
        return S_OK(directoryID)

  def removeDirectory(self,directory):
    """ Remove the directory from all tables in the database
    """
    res = self.__getDirectoryID(directory)
    if not res['OK']:
      return res
    elif not res['Value']:
      return S_OK()
    else:
      directoryID = res['Value']
      failed = False
      req = "DELETE FROM DirectoryUsage WHERE DirectoryID = %s;" % directoryID
      res = self._update(req)
      if not res['OK']:
        failed = True
        err = res['Message']
      req = "DELETE FROM DirectoryParameters WHERE DirectoryID = %s;" % directoryID
      res = self._update(req)
      if not res['OK']:
        failed = True
        err = res['Message']
      req = "DELETE FROM Directory WHERE DirectoryID = %s;" % directoryID
      res = self._update(req)
      if not res['OK']:
        failed = True
        err = res['Message']
      if failed:
        return S_ERROR(errStr)
      else:
        return S_OK()

  #############################################################################
  #
  # Methods for manipulating the Directory table
  #

  def __insertDirectory(self,directory,directoryFiles,directorySize):
    """ Inserts the directory into the Directory table
    """
    req = "INSERT INTO Directory VALUES (Directory,DirectoryFiles,DirectorySize) VALUES ('%s',%s,%s);" % (directory,directoryFiles,directorySize)
    err = "StorageUsageDB.__insertDirectory: Failed to insert directory."
    self.getIdLock.acquire()
    res = self._update(req)
    if not res['OK']:
      self.getIdLock.release()
      return S_ERROR("%s %s" % err,res['Message'])
    req = "SELECT MAX(DirectoryID) FROM Directory;"
    res = self._query(req)
    self.getIdLock.release()
    if not res['OK']:
      err = "StorageUsageDB.__insertDirectory: Failed to get DirectoryID from Directory table."
      return S_ERROR("%s %s" % err,res['Message'])
    if not res['Value']:
      err = "StorageUsageDB.__insertDirectory: Directory details don't appear in Directory table."
      return S_ERROR(err)
    directoryID = res['Value'][0][0]
    return S_OK(directoryID)

  def __getDirectoryID(self,directory):
    """ Obtain the directoryID from the Directory table
    """
    req = "SELECT DirectoryID from Directory WHERE Directory = '%s';" % directory
    err = "StorageUsageDB.__getDirectoryID: Failed to determine directoryID."
    res = self._query(req)
    if not res['OK']:
      return S_ERROR(err,res['Message'])
    elif res['Value']:
      return S_OK(res['Value'][0])
    else:
      return S_OK(False)

  #############################################################################
  #
  # Methods for manipulating the DirectoryParameters table
  #

  def __insertDirectoryParam(self, directoryID, parameter):
    """ Inserts the parameters for the directoryID to the DirectoryParameters table
    """
    req = "INSERT INTO DirectoryParameters (DirectoryID,Parameter) VALUES (%s,'%s');" % (directoryID,parameter)
    res = self._update(req)
    return res

  #############################################################################
  #
  # Methods for manipulating the DirectoryUsage table
  #

  def __checkDirectoryUsage(self,directoryID,storageElement):
    """ Checks the pre-existance of the directoryID,storageElement pair in the DirectoryUsage table
    """
    req = "SELECT DirectoryID from DirectoryUsage WHERE DirectoryID = %s and StorageElement = '%s';" % (directoryID,storageElement)
    err = "StorageUsageDB.__checkDirectoryUsage: Failed to determine existence."
    res = self._query(req)
    if not res['OK']:
      return S_ERROR(err,res['Message'])
    if res['Value']:
      return S_OK(True)
    else:
      return S_OK(False)

  def __insertDirectoryUsage(self, directoryID,storageElement,storageElementSize,storageElementFiles):
    """ Inserts the usage for the directoryID into the DirectoryUsage table
    """
    req = "INSERT INTO DirectoryUsage (DirectoryID,StorageElement,StorageElementSize,StorageElementFiles,Updated) \
           VALUES (%s,'%s',%s,%s,NOW());" % (directoryID,storageElement,storageElementSize,storageElementFiles)
    res = self._update(req)
    return res

  def __updateDirectoryUsage(self,directoryID,storageElement,storageElementSize,storageElementFiles):
    """ Updates an existing entry in the site usage
    """
    req = "UPDATE DirectoryUsage SET StorageElementSize = %s, StorageElementFiles = %s WHERE DirectoryID = %s and StorageElement = '%s';" % (storageElementSize,storageElementFiles,directoryID,storageElement)
    res = self._update(req)
    return res

  def publishDirectoryUsage(self,directory,storageElement,storageElementSize,storageElementFiles):
    """ Publish the usage of the directory
    """
    res = self.__getDirectoryID(directory)
    if not res['OK']:
      return res
    elif not res['Value']:
      return S_ERROR("StorageUsageDB.publishDirectoryUsage: Directory does not exist.")
    else:
      directoryID = res['Value']
      res = self.__checkDirectoryUsage(directoryID,storageElement)
      if not res['OK']:
        return res
      elif not res['Value']:
        res = self.__insertDirectoryUsage(directoryID,storageElement,storageElementSize,storageElementFiles)
      else:
        res = self.__updateDirectoryUsage(directoryID,storageElement,storageElementSize,storageElementFiles)
      return res

  #############################################################################
  #
  # Methods for retreiving storage usage (still to be developed)
  #

  def __getLFNDirSiteDict(self,dirs,sites):
    """ Performs sql query to get site usage for provided directories and sites
    """
    pass

  def __getSites(self,types,prodids,lumis):
    """ Performs the SQL queries to get the sites for the supplied options
    """
    pass

  def __getDirsForOptions(self,types,prodids,lumis):
    """ Performs the SQL queries to get directories from Parameters table
    """
    pass

  def __getOptions(self,types,prodids,lumis):
    """ Performs the SQL queries to get all the available parameters
    """
    pass