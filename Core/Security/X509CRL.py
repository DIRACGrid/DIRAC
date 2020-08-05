""" X509CRL is a class for managing X509CRL
This class is used to manage the revoked certificates....
"""
__RCSID__ = "$Id$"


import stat
import os
import tempfile

from GSI import crypto
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities import DErrno

class X509CRL( object ):

  def __init__( self, cert = None ):

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
      return S_ERROR( DErrno.EOF, "%s: %s" % ( crlLocation, repr( e ).replace( ',)', ')' ) ) )
    return self.loadChainFromString( pemData )

  # Make an alias for compatibility with the M2Crypto implementation
  loadCRLFromFile = loadChainFromFile

  def loadChainFromString( self, pemData ):
    """
    Load a x509CRL certificate from a string containing the pem data
    Return : S_OK / S_ERROR
    """
    self.__loadedCert = False
    try:
      self.__revokedCert = crypto.load_crl( crypto.FILETYPE_PEM, pemData )
    except Exception as e:
      return S_ERROR( DErrno.ECERTREAD, "%s" % repr( e ).replace( ',)', ')' ) )
    if not self.__revokedCert:
      return S_ERROR( DErrno.ECERTREAD )
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
      return S_ERROR( DErrno.EOF, "%s: %s" % ( crlLocation, repr( e ).replace( ',)', ')' ) ) )
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
      return S_ERROR( DErrno.ECERTREAD, "No certificate loaded" )

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
      return S_ERROR( DErrno.EWF, "%s: %s" % ( filename, repr( e ).replace( ',)', ')' ) ) )
    try:
      os.chmod( filename, stat.S_IRUSR | stat.S_IWUSR )
    except Exception as e:
      return S_ERROR( DErrno.ESPF, "%s: %s" % ( filename, repr( e ).replace( ',)', ')' ) ) )
    return S_OK( filename )

  def __str__( self ):
    repStr = "<X509CRL"
    if self.__loadedCert:
      repStr += self.__revokedCert.get_issuer().one_line()
    repStr += ">"
    return repStr

  def __repr__( self ):
    return self.__str__()
