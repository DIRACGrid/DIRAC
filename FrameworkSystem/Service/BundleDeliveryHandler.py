########################################################################
# $HeadURL$
########################################################################

""" ProxyManager is the implementation of the ProxyManagement service
    in the DISET framework
"""

__RCSID__ = "$Id$"

import types
import os
import cStringIO
import tarfile
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import gLogger, S_OK, S_ERROR, gConfig
from DIRAC.Core.Utilities.ThreadScheduler import gThreadScheduler
from DIRAC.Core.Utilities import File, List
from DIRAC.Core.Security import Locations

class BundleManager:

  def __init__( self, baseCSPath ):
    self.__csPath = baseCSPath
    self.__bundles = {}
    self.updateBundles()

  def __getDirsToBundle( self ):
    dirsToBundle = {}
    result = gConfig.getOptionsDict( "%s/DirsToBundle" % self.__csPath )
    if result[ 'OK' ]:
      dB = result[ 'Value' ]
      for bId in dB:
        dirsToBundle[ bId ] = List.fromChar( dB[ bId ] )
    if gConfig.getValue( "%s/BundleCAs" % self.__csPath, True ):
      dirsToBundle[ 'CAs' ] = [ "%s/*.0" % Locations.getCAsLocation(), "%s/*.signing_policy" % Locations.getCAsLocation(), "%s/*.pem" % Locations.getCAsLocation() ]
    if gConfig.getValue( "%s/BundleCRLs" % self.__csPath, True ):
      dirsToBundle[ 'CRLs' ] = [ "%s/*.r0" % Locations.getCAsLocation() ]
    return dirsToBundle

  def getBundles( self ):
    return dict( [ ( bId, self.__bundles[ bId ] )  for bId in self.__bundles ] )

  def bundleExists( self, bId ):
    return bId in self.__bundles

  def getBundleVersion( self, bId ):
    try:
      return self.__bundles[ bId ][0]
    except:
      return ""

  def getBundleData( self, bId ):
    try:
      return self.__bundles[ bId ][1]
    except:
      return ""

  def updateBundles( self ):
    dirsToBundle = self.__getDirsToBundle()
    #Delete bundles that don't have to be updated
    for bId in self.__bundles:
      if bId not in dirsToBundle:
        gLogger.info( "Deleting old bundle %s" % bId )
        del( self.__bundles[ bId ] )
    for bId in dirsToBundle:
      bundlePaths = dirsToBundle[ bId ]
      gLogger.info( "Updating %s bundle %s" % ( bId, bundlePaths ) )
      buffer_ = cStringIO.StringIO()
      filesToBundle = sorted( File.getGlobbedFiles( bundlePaths ) )
      if filesToBundle:
        commonPath = File.getCommonPath( filesToBundle )
        commonEnd = len( commonPath )
        gLogger.info( "Bundle will have %s files with common path %s" % ( len( filesToBundle ), commonPath ) )
        tarBuffer = tarfile.open( 'dummy', "w:gz", buffer_ )
        for filePath in filesToBundle:
          tarBuffer.add( filePath, filePath[ commonEnd: ] )
        tarBuffer.close()
        zippedData = buffer_.getvalue()
        buffer_.close()
        hash_ = File.getMD5ForFiles( filesToBundle )
        gLogger.info( "Bundled %s : %s bytes (%s)" % ( bId, len( zippedData ), hash_ ) )
        self.__bundles[ bId ] = ( hash_, zippedData )
      else:
        self.__bundles[ bId ] = ( None, None )  

gBundleManager = False

def initializeBundleDeliveryHandler( serviceInfoDict ):
  global gBundleManager
  csPath = serviceInfoDict[ 'serviceSectionPath' ]
  gBundleManager = BundleManager( csPath )
  updateBundleTime = gConfig.getValue( "%s/BundlesLifeTime" % csPath, 3600 * 6 )
  gLogger.info( "Bundles will be updated each %s secs" % updateBundleTime )
  gThreadScheduler.addPeriodicTask( updateBundleTime, gBundleManager.updateBundles )
  return S_OK()


class BundleDeliveryHandler( RequestHandler ):

  types_getListOfBundles = []
  def export_getListOfBundles( self ):
    global gBundleManager
    return S_OK( gBundleManager.getBundles() )

  def transfer_toClient( self, fileId, token, fileHelper ):
    global gBundleManager
    version = ""
    if type( fileId ) in ( types.StringType, types.UnicodeType ):
      bId = fileId
    elif type( fileId ) in ( types.ListType, types.TupleType ):
      if len( fileId ) == 0:
        fileHelper.markAsTransferred()
        return S_ERROR( "No bundle specified!" )
      elif len( fileId ) == 1:
        bId = fileId[0]
      else:
        bId = fileId[0]
        version = fileId[1]
    if not gBundleManager.bundleExists( bId ):
      fileHelper.markAsTransferred()
      return S_ERROR( "Unknown bundle %s" % bId )

    bundleVersion = gBundleManager.getBundleVersion( bId )
    if bundleVersion is None:
      fileHelper.markAsTransferred()
      return S_ERROR( "Empty bundle %s" % bId )
    
    if version == bundleVersion:
      fileHelper.markAsTransferred()
      return S_OK( bundleVersion )

    buffer_ = cStringIO.StringIO( gBundleManager.getBundleData( bId ) )
    result = fileHelper.DataSourceToNetwork( buffer_ )
    buffer_.close()
    if not result[ 'OK' ]:
      return result
    return S_OK( bundleVersion )
