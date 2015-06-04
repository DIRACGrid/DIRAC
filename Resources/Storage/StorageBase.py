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
from DIRAC.Resources.Utilities import checkArgumentFormat

import os
PROTOCOL_PARAMETERS = [ "Protocol", "Host", "Path", "Port", "SpaceToken", "WSUrl" ] 

class StorageBase( object ):
  """
  .. class:: StorageBase
  
  """

  def __init__( self, name, parameterDict ):
        
    self.name = name
    self.pluginName = ''
    self.protocolParameters = {}
    
    self.__updateParameters( parameterDict )
    
    self.basePath = parameterDict['Path']
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
    parameterDict["PluginName"] = self.pluginName
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
    urlDict = dict( self.protocolParameters )
    if not fileName.startswith( '/' ):
      # Relative path is given
      urlDict['Path'] = self.cwd
    result = pfnunparse( urlDict )
    if not result['OK']:
      return result
    cwdUrl = result['Value']
    fullUrl = '%s%s' % ( cwdUrl, fileName )
    return S_OK( fullUrl )
    
  def getName( self ):
    """ The name with which the storage was instantiated
    """
    return self.name

  def getURLBase( self, withWSUrl = False ):
    """ This will get the URL base. This is then appended with the LFN in DIRAC convention.

    :param self: self reference
    :param bool withWSUrl: flag to include Web Service part of the url
    :returns URL
    """
    urlDict = dict( self.protocolParameters )
    if not withWSUrl:
      urlDict['WSUrl'] = ''  
    return pfnunparse( urlDict )
  
  def isURL( self, path ):
    """ Guess if the path looks like a URL

    :param self: self reference
    :param string path: input file LFN or URL
    :returns boolean: True if URL, False otherwise
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
        
  def getTransportURL( self, pathDict, protocols ):
    """ Get a transport URL for a given URL. For a simple storage plugin
    it is just returning input URL if the plugin protocol is one of the 
    requested protocols
    
    :param dict pathDict: URL obtained from File Catalog or constructed according
                    to convention
    :param list protocols: a list of acceptable transport protocols in priority order                
    """
    res = checkArgumentFormat( pathDict )
    if not res['OK']:
      return res
    urls = res['Value']
    successful = {}
    failed = {}
    
    if protocols and not self.protocolParameters['Protocol'] in protocols:
      return S_ERROR( 'No native protocol requested' )  
    
    for url in urls:
      successful[url] = url
      
    resDict = {'Failed':failed, 'Successful':successful}    
    return S_OK( resDict )
  
  def constructURLFromLFN( self, lfn, withWSUrl = False ):
    """ Construct URL from the given LFN according to the VO convention for the
    primary protocol of the storage plagin
    
    :param str lfn: file LFN
    :param boolean withWSUrl: flag to include the web service part into the resulting URL
    :return result: result['Value'] - resulting URL     
    """
  
    # Check the LFN convention:
    # 1. LFN must start with the VO name as the top level directory
    # 2. VO name must not appear as any subdirectory or file name
    lfnSplitList = lfn.split( '/' )
    voLFN = lfnSplitList[1]
    # TODO comparison to Sandbox below is for backward compatibility, should be removed in the next release
    if ( voLFN != self.se.vo and voLFN != "SandBox" and voLFN != "Sandbox" ) or self.se.vo in lfnSplitList[2:]:
      
      return S_ERROR( 'LFN does not follow the DIRAC naming convention %s' % lfn )
    
    result = self.getURLBase( withWSUrl = withWSUrl )
    if not result['OK']:
      return result
    urlBase = result['Value']
    url = os.path.join( urlBase, lfn.lstrip( '/' ) )
    return S_OK( url )    
  
  def updateURL( self, url, withWSUrl = False ):
    """ Update the URL according to the current SE parameters
    """
    result = pfnparse( url )
    if not result['OK']:
      return result
    urlDict = result['Value']
    
    urlDict['Protocol'] = self.protocolParameters['Protocol']
    urlDict['Host'] = self.protocolParameters['Host']
    urlDict['Port'] = self.protocolParameters['Port']
    urlDict['WSUrl'] = ''
    if withWSUrl:
      urlDict['WSUrl'] = self.protocolParameters['WSUrl']
      
    return pfnunparse( urlDict )
  
  def isNativeURL( self, url ):
    """ Check if URL :url: is valid for :self.protocol:

    :param self: self reference
    :param str url: URL
    """
    res = pfnparse( url )
    if not res['OK']:
      return res
    urlDict = res['Value']
    return S_OK( urlDict['Protocol'] == self.protocolParameters['Protocol'] )
  
