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
import types

class FileCatalogueBase(object):

  def __init__( self, name = '' ):
    self.name = name

  def setName( self, name ):
    self.name = name

  def exists( self, path ):
    if type( path ) == types.StringType:
      paths = [path]
    else:
      paths = path
    return S_ERROR( 'Implement me' )

  ####################################################################
  #
  # These are the methods for file manipulation
  #

  def isFile( self, path ):
    if type( path ) == types.StringType:
      paths = [path]
    else:
      paths = path
    return S_ERROR( 'Implement me' )

  def removeFile( self, path ):
    if type( path ) == types.StringType:
      paths = [path]
    else:
      paths = path
    return S_ERROR( 'Implement me' )

  def addFile( self, fileTuple ):
    """ A tuple should be supplied to this method which contains:
        (lfn,pfn,size,se,guid)
        A list of tuples may also be supplied.
    """
    if type( fileTuple ) == types.TupleType:
      files = [fileTuple]
    else:
      files = fileTuple
    for fileTuple in files:
      lfn, pfn, size, se, guid = fileTuple
    return S_ERROR( 'Implement me' )

  def addReplica( self, replicaTuple ):
    if type( replicaTuple ) == types.TupleType:
      replicas = [replicaTuple]
    else:
      replicas = replicaTuple
    for replicaTuple in replicas:
      lfn, pfn, se = replicaTuple
    return S_ERROR( 'Implement me' )

  def getFileMetadata( self, path, ownership = False ):
    if type( path ) == types.StringType:
      paths = [path]
    else:
      paths = path
    return S_ERROR( 'Implement me' )

  def getReplicas( self, path, directory = 0 ):
    if type( path ) == types.StringType:
      paths = [path]
    else:
      paths = path
    return S_ERROR( 'Implement me' )

  def removeReplica( self, replicaTuple ):
    if type( replicaTuple ) == types.TupleType:
      replicas = [replicaTuple]
    else:
      replicas = replicaTuple
    for replicaTuple in replicas:
      lfn, se = replicaTuple
    return S_ERROR( 'Implement me' )

  def getReplicaStatus( self, replicaTuple ):
    if type( replicaTuple ) == types.TupleType:
      replicas = [replicaTuple]
    else:
      replicas = replicaTuple
    for replicaTuple in replicas:
      lfn, se = replicaTuple
    return S_ERROR( 'Implement me' )

  def setReplicaStatus( self, replicaTuple ):
    if type( replicaTuple ) == types.TupleType:
      replicas = [replicaTuple]
    elif type( replicaTuple ) == types.ListType:
      replicas = replicaTuple
    else:
      return S_ERROR( 'LFCClient.setReplicaStatus: Must supply a file tuple or list of file typles' )
    return S_ERROR( 'Implement me' )

  def setReplicaHost( self, replicaTuple ):
    """ This modifies the replica metadata for the SE and space token.
        The tuple supplied must be of the following form:
        (lfn,pfn,se,spaceToken)
    """
    if type( replicaTuple ) == types.TupleType:
      replicas = [replicaTuple]
    elif type( replicaTuple ) == types.ListType:
      replicas = replicaTuple
    else:
      return S_ERROR( 'LFCClient.setReplicaHost: Must supply a file tuple or list of file typles' )
    return S_ERROR( 'Implement me' )

  def getFileSize( self, path ):
    if type( path ) == types.StringType:
      paths = [path]
    else:
      paths = path
    return S_ERROR( 'Implement me' )

  ####################################################################
  #
  # These are the methods for directory manipulation
  #

  def createDirectory( self, path ):
    if type( path ) == types.StringType:
      paths = [path]
    else:
      paths = path
    return S_ERROR( 'Implement me' )

  def isDirectory( self, path ):
    if type( path ) == types.StringType:
      paths = [path]
    else:
      paths = path
    return S_ERROR( 'Implement me' )

  def listDirectory( self, path ):
    if type( path ) == types.StringType:
      paths = [path]
    else:
      paths = path
    return S_ERROR( 'Implement me' )

  def removeDirectory( self, path, recursive = False ):
    if type( path ) == types.StringType:
      paths = [path]
    else:
      paths = path
    return S_ERROR( 'Implement me' )

  def getDirectoryReplicas( self, path, allStatus = False ):
    if type( path ) == types.StringType:
      paths = [path]
    else:
      paths = path
    return S_ERROR( 'Implement me' )

  def getDirectoryMetadata( self, path ):
    if type( path ) == types.StringType:
      paths = [path]
    else:
      paths = path
    return S_ERROR( 'Implement me' )

  def getDirectorySize( self, path ):
    if type( path ) == types.StringType:
      paths = [path]
    else:
      paths = path
    return S_ERROR( 'Implement me' )

  ####################################################################
  #
  # These are the methods for link manipulation
  #

  def isLink( self, path ):
    if type( path ) == types.StringType:
      paths = [path]
    else:
      paths = path
    return S_ERROR( 'Implement me' )

  def createLink( self, path ):
    if type( path ) == types.StringType:
      paths = [path]
    else:
      paths = path
    return S_ERROR( 'Implement me' )

  def removeLink( self, path ):
    if type( path ) == types.StringType:
      paths = [path]
    else:
      paths = path
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
