""" Base Storage Class provides the base interface for all storage plug-ins

      exists()

These are the methods for manipulating files:
      isFile()
      getFile()
      putFile()
      removeFile()
      getFileMetadata()
      getFileSize()
      prestageFile()
      getTransportURL()

These are the methods for manipulating directories:
      isDirectory()
      getDirectory()
      putDirectory()
      createDirectory()
      removeDirectory()
      listDirectory()
      getDirectoryMetadata()
      getDirectorySize()

These are the methods for manipulating the client:
      changeDirectory()
      getCurrentDirectory()
      getName()
      getParameters()
      getProtocolPfn()
      getCurrentURL()
"""

from DIRAC import gLogger, gConfig, S_OK, S_ERROR
import re

class StorageBase:

  def __init__(self,name,rootdir):
    self.name = name
    self.rootdir = rootdir
    self.cwd = self.rootdir

  def exists(self,path):
    """Check if the given path exists
    """
    return S_ERROR("Storage.exists: implement me!")

  #############################################################
  #
  # These are the methods for file manipulation
  #

  def isFile(self,fname):
    """Check if the given path exists and it is a file
    """
    return S_ERROR("Storage.isFile: implement me!")

  def getFile(self,path):
    """Get a local copy of the file specified by its path
    """
    return S_ERROR("Storage.getFile: implement me!")

  def putFile(self,fname):
    """Put a copy of the local file to the current directory on the
       physical storage
    """
    return S_ERROR("Storage.putFile: implement me!")

  def removeFile(self,path):
    """Remove physically the file specified by its path
    """
    return S_ERROR("Storage.removeFile: implement me!")

  def getFileMetadata(self,path):
    """  Get metadata associated to the file
    """
    return S_ERROR("Storage.getFileMetadata: implement me!")

  def getFileSize(self,fname):
    """Get the physical size of the given file
    """
    return S_ERROR("Storage.getFileSize: implement me!")

  def prestageFile(self,path):
    """ Issue prestage request for file
    """
    return S_ERROR("Storage.prestageFile: implement me!")

  def getTransportURL(self,path,protocols):
    """ Obtain the TURLs for the supplied path and protocols
    """
    return S_ERROR("Storage.getTransportURL: implement me!")

  #############################################################
  #
  # These are the methods for directory manipulation
  #

  def isDirectory(self,dirname):
    """Check if the given path exists and it is a directory
    """
    return S_ERROR("Storage.isDirectory: implement me!")

  def getDirectory(self,path):
    """Get locally a directory from the physical storage together with all its
       files and subdirectories.
    """
    return S_ERROR("Storage.getDirectory: implement me!")

  def putDirectory(self,path):
    """Put a local directory to the physical storage together with all its
       files and subdirectories.
    """
    return S_ERROR("Storage.putDirectory: implement me!")

  def createDirectory(self,newdir):
    """ Make a new directory on the physical storage
    """
    return S_ERROR("Storage.createDirectory: implement me!")

  def removeDirectory(self,path):
    """Remove a directory on the physical storage together with all its files and
       subdirectories.
    """
    return S_ERROR("Storage.removeDirectory: implement me!")

  def listDirectory(self,path):
    """ List the supplied path
    """
    return S_ERROR("Storage.listDirectory: implement me!")

  def getDirectoryMetadata(self,path):
    """ Get the metadata for the directory
    """
    return S_ERROR("Storage.getDirectoryMetadata: implement me!")

  def getDirectorySize(self,path):
    """ Get the size of the directory on the storage
    """
    return S_ERROR("Storage.getDirectorySize: implement me!")


  #############################################################
  #
  # These are the methods for manipulting the client
  #

  def changeDirectory(self,newdir):
    """ Change the current directory
    """
    self.cwd = newdir
    return S_OK()

  def getCurrentDirectory(self):
    """ Get the current directory
    """
    return S_OK(self.cwd)

  def getName(self):
    """ The name with which the storage was instantiated
    """
    return S_OK(self.name)

  def getParameters(self):
    """ Get the parameters with which the storage was instantiated
    """
    return S_ERROR("Storage.getParameters: implement me!")

  def getProtocolPfn(self,pfnDict,withPort):
    """ Get the PFN for the protocol with or without the port
    """
    return S_ERROR("Storage.getProtocolPfn: implement me!")

  def getCurrentURL(self,fileName):
    """ Create the full URL for the storage using the configuration, self.cwd and the fileName
    """
    return S_ERROR("Storage.getCurrentURL: implement me!")
