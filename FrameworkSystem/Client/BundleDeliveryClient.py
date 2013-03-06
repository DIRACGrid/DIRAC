
import os
import tarfile
import cStringIO
from DIRAC import S_OK, gLogger
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.DISET.TransferClient import TransferClient
from DIRAC.Core.Security import Locations, CS

class BundleDeliveryClient:

  def __init__( self, rpcClient = False, transferClient = False ):
    self.rpcClient = rpcClient
    self.transferClient = transferClient
    self.log = gLogger.getSubLogger( "BundleDelivery" )

  def __getRPCClient( self ):
    if self.rpcClient:
      return self.rpcClient
    return RPCClient( "Framework/BundleDelivery",
                      skipCACheck = CS.skipCACheck() )

  def __getTransferClient( self ):
    if self.transferClient:
      return self.transferClient
    return TransferClient( "Framework/BundleDelivery",
                           skipCACheck = CS.skipCACheck() )

  def __getHash( self, bundleID, dirToSyncTo ):
    try:
      fd = open( os.path.join( dirToSyncTo, ".dab.%s" % bundleID ), "rb" )
      hash = fd.read().strip()
      fd.close()
      return hash
    except:
      return ""

  def __setHash( self, bundleID, dirToSyncTo, hash ):
    try:
      fileName = os.path.join( dirToSyncTo, ".dab.%s" % bundleID )
      fd = open( fileName, "wb" )
      fd.write( hash )
      fd.close()
    except Exception, e:
      self.log.error( "Could not save hash after synchronization", "%s: %s" % ( fileName, str( e ) ) )

  def syncDir( self, bundleID, dirToSyncTo ):
    dirCreated = False
    if not os.path.isdir( dirToSyncTo ):
      self.log.info( "Creating dir %s" % dirToSyncTo )
      os.makedirs( dirToSyncTo )
      dirCreated = True
    currentHash = self.__getHash( bundleID, dirToSyncTo )
    self.log.info( "Current hash for bundle %s in dir %s is '%s'" % ( bundleID, dirToSyncTo, currentHash ) )
    buff = cStringIO.StringIO()
    transferClient = self.__getTransferClient()
    result = transferClient.receiveFile( buff, ( bundleID, currentHash ) )
    if not result[ 'OK' ]:
      self.log.error( "Could not sync dir", result[ 'Message' ] )
      if dirCreated:
        self.log.info( "Removing dir %s" % dirToSyncTo )
        os.unlink( dirToSyncTo )
      buff.close()
      return result
    newHash = result[ 'Value' ]
    if newHash == currentHash:
      self.log.info( "Dir %s was already in sync" % dirToSyncTo )
      return S_OK( False )
    buff.seek( 0 )
    self.log.info( "Synchronizing dir with remote bundle" )
    tF = tarfile.open( name = 'dummy', mode = "r:gz", fileobj = buff )
    for tarinfo in tF:
      tF.extract( tarinfo, dirToSyncTo )
    tF.close()
    buff.close()
    self.__setHash( bundleID, dirToSyncTo, newHash )
    self.log.info( "Dir has been synchronized" )
    return S_OK( True )

  def syncCAs( self ):
    X509_CERT_DIR = False
    if 'X509_CERT_DIR' in os.environ:
      X509_CERT_DIR = os.environ['X509_CERT_DIR']
      del os.environ['X509_CERT_DIR']
    casLocation = Locations.getCAsLocation()
    if not casLocation:
      casLocation = Locations.getCAsDefaultLocation()
    result = self.syncDir( "CAs", casLocation )
    if X509_CERT_DIR:
      os.environ['X509_CERT_DIR'] = X509_CERT_DIR
    return result

  def syncCRLs( self ):
    X509_CERT_DIR = False
    if 'X509_CERT_DIR' in os.environ:
      X509_CERT_DIR = os.environ['X509_CERT_DIR']
      del os.environ['X509_CERT_DIR']
    result = self.syncDir( "CRLs", Locations.getCAsLocation() )
    if X509_CERT_DIR:
      os.environ['X509_CERT_DIR'] = X509_CERT_DIR
    return result
