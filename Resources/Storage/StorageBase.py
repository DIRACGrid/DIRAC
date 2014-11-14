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
      getCurrentURL()
"""
__RCSID__ = "$Id$"

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities.Pfn import pfnparse, pfnunparse

PROTOCOL_PARAMETERS = [ "Protocol", "Host", "Path", "Port", "SpaceToken", "WSUrl" ] 

class StorageBase:
  """
  .. class:: StorageBase
  
  """

  def __init__( self, name, parameterDict ):
        
    self.name = name
    self.protocolName = ''
    self.protocolParameters = {}
    self.basePath = parameterDict['BasePath']
    
    self.__updateParameters( parameterDict )
    
    self.isok = True
    self.cwd = self.basePath
    self.se = None
    
  def setStorageElement( self, se ):
    self.se = se  

  def setParameters( self, parameterDict ):
    """ Set standard parameters, method can be overriden in subclasses
        to process specific parameters
    """
    self.__updateParameters( parameterDict )

  def __updateParameters( self, parameterDict ):
    """ setParameters implementation method
    """
    for item in PROTOCOL_PARAMETERS:
      self.protocolParameters[item] = parameterDict.get( item, '' )
  
  def getParameters( self ):
    """ Get the parameters with which the storage was instantiated
    """
    parameterDict = dict( self.protocolParameters )
    parameterDict["StorageName"] = self.name
    parameterDict["ProtocolName"] = self.protocolName
    return parameterDict    

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
  # These are the methods for manipulating the client
  #

  def isOK( self ):
    return self.isok

  def resetCurrentDirectory( self ):
    """ Reset the working directory to the base dir
    """
    self.cwd = self.basePath

  def changeDirectory( self, directory ):
    """ Change the directory to the supplied directory
    """
    if directory.startswith( '/' ):
      self.cwd = "%s/%s" % ( self.basePath, directory )
    else:
      self.cwd = '%s/%s' % ( self.cwd, directory )

  def getCurrentDirectory( self ):
    """ Get the current directory
    """
    return self.cwd
  
  def getCurrentURL( self, fileName ):
    """ Obtain the current file URL from the current working directory and the filename

    :param self: self reference
    :param str fileName: path on storage
    """
    if fileName.startswith( '/' ):
      # Assume full path is given, e.g. LFN
      return self.getPfn( fileName )
    
    pfnDict = dict( self.protocolParameters )
    pfnDict['Path'] = self.cwd
    result = pfnunparse( pfnDict )
    if not result['OK']:
      return result
    cwdUrl = result['Value']
    fullUrl = '%s/%s' % ( cwdUrl, fileName )
    return S_OK( fullUrl )
    
  def getName( self ):
    """ The name with which the storage was instantiated
    """
    return self.name

  def getPFNBase( self, withPort = False ):
    """ This will get the pfn base. This is then appended with the LFN in DIRAC convention.

    :param self: self reference
    :param bool withPort: flag to include port
    :returns PFN
    """
    pfnDict = dict( self.protocolParameters )
    if not withPort:
      pfnDict['Port'] = ''  
      pfnDict['WSUrl'] = ''  
    return pfnunparse( pfnDict )
  
  def isPfn( self, path ):
    """ Guess if the path looks like a PFN

    :param self: self reference
    :param string path: input file LFN or PFN
    :returns boolean: True if PFN, False otherwise
    """
    if self.basePath and path.startswith( self.basePath ):
      return S_OK( True )

    result = pfnparse( path )
    if not result['OK']:
      return result

    if len( result['Value']['Protocol'] ) != 0:
      return S_OK( True )

    if result['Value']['Path'].startswith( self.basePath ):
      return S_OK( True )

    return S_OK( False )
  
  def getPfn( self, lfn, withPort = False ):
    """ Construct PFN from the given LFN according to the VO convention 
    """
    
    result = self.isPfn( lfn )
    if not result['OK']:
      return result
    
    # If we are given a PFN, update it
    if result['Value']:
      return self.updatePfn( lfn, withPort = withPort )
    
    # Check the LFN convention
    voLFN = lfn.split( '/' )[1]
    if voLFN != self.se.vo:
      return S_ERROR( 'LFN does not follow the DIRAC naming convention %s' % lfn )
    
    result = self.getPFNBase( withPort = withPort )
    if not result['OK']:
      return result
    pfnBase = result['Value']
    # Strip of the top level directory from LFN corresponding to VO
    # and merge with the pfn base containing the VOPath
    pfn = '%s/%s' % ( pfnBase, '%s' % '/'.join( lfn.split( '/' )[2:] ) )    
    return S_OK( pfn )    
  
  def updatePfn( self, pfn, withPort = False ):
    """ Update the PFN according to the current SE parameters
    """
    result = pfnparse( pfn )
    if not result['OK']:
      return result
    pfnDict = result['Value']
    
    pfnDict['Protocol'] = self.protocolParameters['Protocol']
    pfnDict['Host'] = self.protocolParameters['Host']
    if withPort:
      pfnDict['Port'] = self.protocolParameters['Port']
      pfnDict['WSUrl'] = self.protocolParameters['WSUrl']
    else:
      pfnDict['Port'] = ''
      pfnDict['WSUrl'] = ''
    return pfnunparse( pfnDict )
  
  def isNativePfn( self, pfn ):
    """ Check if PFN :pfn: is valid for :self.protocol:

    :param self: self reference
    :param str pfn: PFN
    """
    res = pfnparse( pfn )
    if not res['OK']:
      return res
    pfnDict = res['Value']
    return S_OK( pfnDict['Protocol'] == self.protocolParameters['Protocol'] )
  
