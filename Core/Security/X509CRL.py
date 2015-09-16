""" X509CRL is a class for managing X509CRL
This class is used to manage the revoked certificates....
"""
__RCSID__ = "$Id$"


import stat
import os
import tempfile

from GSI import crypto
from DIRAC import S_OK, S_ERROR, gLogger

class X509CRL( object ):

  def __init__( self, cert = False ):

    self.__pemData = None

    if cert:
      self.__loadedCert = True
      self.__revokedCert = cert
    else:
      self.__loadedCert = False


  @classmethod
  def instanceFromFile( cls, crlLocation ):
    """ Instance a X509CRL from a file
    """
    chain = cls()
    result = chain.loadChainFromFile( crlLocation )
    if not result[ 'OK' ]:
      return result
    return S_OK( chain )

  def loadChainFromFile( self, crlLocation ):
    """
    Load a x509CRL certificate from a pem file
    Return : S_OK / S_ERROR
    """
    try:
      fd = file( crlLocation )
      pemData = fd.read()
      fd.close()
    except Exception as e:
      gLogger.error( "Can't open file", "%s: %s" % ( crlLocation, str( e ) ) )
      return S_ERROR( "Can't open file" )
    return self.loadChainFromString( pemData )

  def loadChainFromString( self, pemData ):
    """
    Load a x509CRL certificate from a string containing the pem data
    Return : S_OK / S_ERROR
    """
    self.__loadedCert = False
    try:
      self.__revokedCert = crypto.load_crl( crypto.FILETYPE_PEM, pemData )
    except Exception as e:
      gLogger.error( "Can't load pem data", "%s" % str( e ) )
      return S_ERROR( "Can't load pem data" )
    if not self.__revokedCert:
      return S_ERROR( "No certificates in the contents" )
    self.__loadedCert = True
    self.__pemData = pemData

    return S_OK()


  def loadProxyFromFile( self, crlLocation ):
    """
    Load a Proxy from a pem file
    Return : S_OK / S_ERROR
    """
    try:
      fd = file( crlLocation )
      pemData = fd.read()
      fd.close()
    except Exception as e:
      gLogger.error( "Can't open file", "%s: %s" % ( crlLocation, str( e ) ) )
      return S_ERROR( "Can't open file" )
    return self.loadProxyFromString( pemData )

  def loadProxyFromString( self, pemData ):
    """
    Load a Proxy from a pem buffer
    Return : S_OK / S_ERROR
    """
    return self.loadChainFromString( pemData )


  def dumpAllToString( self ):
    """
    Dump all to string
    """
    if not self.__loadedCert:
      gLogger.error( "No certificate loaded" )
      return S_ERROR( "No certificate loaded" )

    return S_OK( self.__pemData )

  def dumpAllToFile( self, filename = False ):
    """
    Dump all to file. If no filename specified a temporal one will be created
    """
    retVal = self.dumpAllToString()
    if not retVal[ 'OK' ]:
      return retVal
    pemData = retVal['Value']
    try:
      if not filename:
        fd, filename = tempfile.mkstemp()
        os.write( fd, pemData )
        os.close( fd )
      else:
        fd = file( filename, "w" )
        fd.write( pemData )
        fd.close()
    except Exception as e:
      gLogger.error( "Cannot write to file", "%s: %s" % ( filename, str( e ) ) )
      return S_ERROR( "Cannot write to file" )
    try:
      os.chmod( filename, stat.S_IRUSR | stat.S_IWUSR )
    except Exception as e:
      gLogger.error( "Cannot set permissions to file", "%s: %s" % ( filename, str( e ) ) )
      return S_ERROR( "Cannot set permissions to file" )
    return S_OK( filename )

  def __str__( self ):
    repStr = "<X509CRL"
    if self.__loadedCert:
      repStr += self.__revokedCert.get_issuer().one_line()
    repStr += ">"
    return repStr

  def __repr__( self ):
    return self.__str__()


