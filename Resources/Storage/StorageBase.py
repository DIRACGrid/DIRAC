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
__RCSID__ = "$Id$"

from DIRAC import S_OK, S_ERROR

class StorageBase:
  """
  .. class:: StorageBase
  
  """

  def __init__( self, name, rootdir ):
    self.isok = True
    self.name = name
    self.rootdir = rootdir
    self.cwd = self.rootdir

  def exists( self, *parms, **kws ):
    """Check if the given path exists
    """
    return S_ERROR( "Storage.exists: implement me!" )

  #############################################################
  #
  # These are the methods for file manipulation
  #

  def isFile( self, *parms, **kws ):
    """Check if the given path exists and it is a file
    """
    return S_ERROR( "Storage.isFile: implement me!" )

  def getFile( self, *parms, **kws ):
    """Get a local copy of the file specified by its path
    """
    return S_ERROR( "Storage.getFile: implement me!" )

  def putFile( self, *parms, **kws ):
    """Put a copy of the local file to the current directory on the
       physical storage
    """
    return S_ERROR( "Storage.putFile: implement me!" )

  def removeFile( self, *parms, **kws ):
    """Remove physically the file specified by its path
    """
    return S_ERROR( "Storage.removeFile: implement me!" )

  def getFileMetadata( self, *parms, **kws ):
    """  Get metadata associated to the file
    """
    return S_ERROR( "Storage.getFileMetadata: implement me!" )

  def getFileSize( self, *parms, **kws ):
    """Get the physical size of the given file
    """
    return S_ERROR( "Storage.getFileSize: implement me!" )

  def getTransportURL( self, *parms, **kws ):
    """ Obtain the TURLs for the supplied path and protocols
    """
    return S_ERROR( "Storage.getTransportURL: implement me!" )

  def prestageFile( self, *parms, **kws ):
    """ Issue prestage request for file
    """
    return S_ERROR( "Storage.prestageFile: implement me!" )

  def prestageFileStatus( self, *parms, **kws ):
    """ Obtain the status of the prestage request
    """
    return S_ERROR( "Storage.prestageFileStatus: implement me!" )

  def pinFile( self, *parms, **kws ):
    """ Pin the file on the destination storage element
    """
    return S_ERROR( "Storage.pinFile: implement me!" )

  def releaseFile( self, *parms, **kws ):
    """ Release the file on the destination storage element
    """
    return S_ERROR( "Storage.releaseFile: implement me!" )

  #############################################################
  #
  # These are the methods for directory manipulation
  #

  def isDirectory( self, *parms, **kws ):
    """Check if the given path exists and it is a directory
    """
    return S_ERROR( "Storage.isDirectory: implement me!" )

  def getDirectory( self, *parms, **kws ):
    """Get locally a directory from the physical storage together with all its
       files and subdirectories.
    """
    return S_ERROR( "Storage.getDirectory: implement me!" )

  def putDirectory( self, *parms, **kws ):
    """Put a local directory to the physical storage together with all its
       files and subdirectories.
    """
    return S_ERROR( "Storage.putDirectory: implement me!" )

  def createDirectory( self, *parms, **kws ):
    """ Make a new directory on the physical storage
    """
    return S_ERROR( "Storage.createDirectory: implement me!" )

  def removeDirectory( self, *parms, **kws ):
    """Remove a directory on the physical storage together with all its files and
       subdirectories.
    """
    return S_ERROR( "Storage.removeDirectory: implement me!" )

  def listDirectory( self, *parms, **kws ):
    """ List the supplied path
    """
    return S_ERROR( "Storage.listDirectory: implement me!" )

  def getDirectoryMetadata( self, *parms, **kws ):
    """ Get the metadata for the directory
    """
    return S_ERROR( "Storage.getDirectoryMetadata: implement me!" )

  def getDirectorySize( self, *parms, **kws ):
    """ Get the size of the directory on the storage
    """
    return S_ERROR( "Storage.getDirectorySize: implement me!" )

  #############################################################
  #
  # These are the methods to get the current storage properties
  #

  def getCurrentStatus(self, *parms, **kws ):
    """ Get the current properties: available disk, usage (for RSS)
    """
    return S_ERROR("Storage.getCurrentStatus: implement me!")

  #############################################################
  #
  # These are the methods for manipulating the client
  #

  def isOK( self ):
    return self.isok

  def changeDirectory( self, newdir ):
    """ Change the current directory
    """
    self.cwd = newdir
    return S_OK()

  def getCurrentDirectory( self ):
    """ Get the current directory
    """
    return S_OK( self.cwd )

  def getName( self ):
    """ The name with which the storage was instantiated
    """
    return S_OK( self.name )
  
  def setParameters( self, parameters ):
    """ Set extra storage parameters, non-mandatory method
    """
    return S_OK()

  def getParameters( self, *parms, **kws ):
    """ Get the parameters with which the storage was instantiated
    """
    return S_ERROR( "Storage.getParameters: implement me!" )

  def getProtocolPfn( self, *parms, **kws ):
    """ Get the PFN for the protocol with or without the port
    """
    return S_ERROR( "Storage.getProtocolPfn: implement me!" )

  def getCurrentURL( self, *parms, **kws ):
    """ Create the full URL for the storage using the configuration, self.cwd and the fileName
    """
    return S_ERROR( "Storage.getCurrentURL: implement me!" )
