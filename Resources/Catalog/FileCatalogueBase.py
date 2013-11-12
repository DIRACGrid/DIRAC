""" Class for FileCatalogueBase

    This outlines the interface that must be exposed by every file catalogue plug in

    exists()

    isFile()
    removeFile()
    addFile()
    addReplica()
    removeReplica()
    getFileMetadata()
    getReplicas()
    getReplicaStatus()
    getReplicaStatus()
    setReplicaHost()
    getFileSize()

    createDirectory()
    isDirectory()
    listDirectory()
    removeDirectory()
    getDirectoryMetadata()
    getDirectorySize()

    isLink()
    createLink()
    removeLink()

    changeDirectory()
    getCurrentDirectory()
    getName()

"""
from DIRAC import S_OK, S_ERROR

class FileCatalogueBase( object ):

  def __init__( self, name = '' ):
    self.name = name

  def setName( self, name ):
    self.name = name

  def exists( self, path ):
    return S_ERROR( 'Implement me' )

  ####################################################################
  #
  # These are the methods for file manipulation
  #

  def isFile( self, path ):
    return S_ERROR( 'Implement me' )

  def removeFile( self, path ):
    return S_ERROR( 'Implement me' )

  def addFile( self, fileTuple ):
    """ A tuple should be supplied to this method which contains:
        (lfn,pfn,size,se,guid)
        A list of tuples may also be supplied.
    """
    return S_ERROR( 'Implement me' )

  def addReplica( self, replicaTuple ):
    return S_ERROR( 'Implement me' )

  def getFileMetadata( self, path, ownership = False ):
    return S_ERROR( 'Implement me' )

  def getReplicas( self, path, directory = 0 ):
    return S_ERROR( 'Implement me' )

  def removeReplica( self, replicaTuple ):
    return S_ERROR( 'Implement me' )

  def getReplicaStatus( self, replicaTuple ):
    return S_ERROR( 'Implement me' )

  def setReplicaStatus( self, replicaTuple ):
    return S_ERROR( 'Implement me' )

  def setReplicaHost( self, replicaTuple ):
    """ This modifies the replica metadata for the SE and space token.
        The tuple supplied must be of the following form:
        (lfn,pfn,se,spaceToken)
    """
    return S_ERROR( 'Implement me' )

  def getFileSize( self, path ):
    return S_ERROR( 'Implement me' )

  ####################################################################
  #
  # These are the methods for directory manipulation
  #

  def createDirectory( self, path ):
    return S_ERROR( 'Implement me' )

  def isDirectory( self, path ):
    return S_ERROR( 'Implement me' )

  def listDirectory( self, path ):
    return S_ERROR( 'Implement me' )

  def removeDirectory( self, path, recursive = False ):
    return S_ERROR( 'Implement me' )

  def getDirectoryReplicas( self, path, allStatus = False ):
    return S_ERROR( 'Implement me' )

  def getDirectoryMetadata( self, path ):
    return S_ERROR( 'Implement me' )

  def getDirectorySize( self, path ):
    return S_ERROR( 'Implement me' )

  ####################################################################
  #
  # These are the methods for link manipulation
  #

  def isLink( self, path ):
    return S_ERROR( 'Implement me' )

  def createLink( self, path ):
    return S_ERROR( 'Implement me' )

  def removeLink( self, path ):
    return S_ERROR( 'Implement me' )

  ####################################################################
  #
  # These are the get/set methods for use within the client
  #

  def changeDirectory( self, path ):
    self.cwd = path
    return S_OK()

  def getCurrentDirectory( self ):
    return S_OK( self.cwd )

  def getName( self ):
    return S_OK( self.name )
