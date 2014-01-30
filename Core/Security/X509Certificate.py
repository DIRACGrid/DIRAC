########################################################################
# $HeadURL$
########################################################################
""" X509Certificate is a class for managing X509 certificates alone
"""
__RCSID__ = "$Id$"

import GSI
import os
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities import Time
from DIRAC.ConfigurationSystem.Client.Helpers import Registry

# Not Used
# def _proxyExtensionList( ):
#   return [ GSI.crypto.X509Extension( 'keyUsage', 'critical, digitalSignature, keyEncipherment, dataEncipherment' ) ]

class X509Certificate:

  def __init__( self, x509Obj = None ):
    self.__valid = False
    if x509Obj:
      self.__certObj = x509Obj
      self.__valid = True

  def load( self, certificate ):
    """ Load a x509 certificate either from a file or from a string
    """

    if os.path.exists( certificate ):
      return self.loadFromFile( certificate )
    else:
      return self.loadFromString( certificate )

  def loadFromFile( self, certLocation ):
    """
    Load a x509 cert from a pem file
    Return : S_OK / S_ERROR
    """
    try:
      fd = file( certLocation )
      pemData = fd.read()
      fd.close()
    except IOError:
      return S_ERROR( "Can't open %s file" % certLocation )
    return self.loadFromString( pemData )

  def loadFromString( self, pemData ):
    """
    Load a x509 cert from a string containing the pem data
    Return : S_OK / S_ERROR
    """
    try:
      self.__certObj = GSI.crypto.load_certificate( GSI.crypto.FILETYPE_PEM, pemData )
    except Exception, e:
      return S_ERROR( "Can't load pem data: %s" % str( e ) )
    self.__valid = True
    return S_OK()

  def setCertificate( self, x509Obj ):
    if type( x509Obj ) != GSI.crypto.X509Type:
      return S_ERROR( "Object %s has to be of type X509" % str( x509Obj ) )
    self.__certObj = x509Obj
    self.__valid = True
    return S_OK()

  def hasExpired( self ):
    """
    Check if a certificate file/proxy is still valid
    Return: S_OK( True/False )/S_ERROR
    """
    if not self.__valid:
      return S_ERROR( "No certificate loaded" )
    return S_OK( self.__certObj.has_expired() )

  def getNotAfterDate( self ):
    """
    Get not after date of a certificate
    Return: S_OK( datetime )/S_ERROR
    """
    if not self.__valid:
      return S_ERROR( "No certificate loaded" )
    return S_OK( self.__certObj.get_not_after() )

  def getNotBeforeDate( self ):
    """
    Get not before date of a certificate
    Return: S_OK( datetime )/S_ERROR
    """
    if not self.__valid:
      return S_ERROR( "No certificate loaded" )
    return S_OK( self.__certObj.get_not_before() )

  def getSubjectDN( self ):
    """
    Get subject DN
    Return: S_OK( string )/S_ERROR
    """
    if not self.__valid:
      return S_ERROR( "No certificate loaded" )
    return S_OK( self.__certObj.get_subject().one_line() )

  def getIssuerDN( self ):
    """
    Get issuer DN
    Return: S_OK( string )/S_ERROR
    """
    if not self.__valid:
      return S_ERROR( "No certificate loaded" )
    return S_OK( self.__certObj.get_issuer().one_line() )

  def getSubjectNameObject( self ):
    """
    Get subject name object
    Return: S_OK( X509Name )/S_ERROR
    """
    if not self.__valid:
      return S_ERROR( "No certificate loaded" )
    return S_OK( self.__certObj.get_subject() )

  def getIssuerNameObject( self ):
    """
    Get issuer name object
    Return: S_OK( X509Name )/S_ERROR
    """
    if not self.__valid:
      return S_ERROR( "No certificate loaded" )
    return S_OK( self.__certObj.get_issuer() )

  def getPublicKey( self ):
    """
    Get the public key of the certificate
    """
    if not self.__valid:
      return S_ERROR( "No certificate loaded" )
    return S_OK( self.__certObj.get_pubkey() )

  def getSerialNumber( self ):
    """
    Get certificate serial number
    Return: S_OK( serial )/S_ERROR
    """
    if not self.__valid:
      return S_ERROR( "No certificate loaded" )
    return S_OK( self.__certObj.get_serial_number() )

  def getDIRACGroup( self, ignoreDefault = False ):
    """
    Get the dirac group if present
    """
    if not self.__valid:
      return S_ERROR( "No certificate loaded" )
    extList = self.__certObj.get_extensions()
    for ext in extList:
      if ext.get_sn() == "diracGroup":
        return S_OK( ext.get_value() )
    if ignoreDefault:
      return S_OK( False )
    result = self.getIssuerDN()
    if not result[ 'OK' ]:
      return result
    return Registry.findDefaultGroupForDN( result['Value'] )

  def hasVOMSExtensions( self ):
    """
    Has voms extensions
    """
    if not self.__valid:
      return S_ERROR( "No certificate loaded" )
    extList = self.__certObj.get_extensions()
    for ext in extList:
      if ext.get_sn() == "vomsExtensions":
        return S_OK( True )
    return S_OK( False )

  def generateProxyRequest( self, bitStrength = 1024, limited = False ):
    """
    Generate a proxy request
    Return S_OK( X509Request ) / S_ERROR
    """
    if not self.__valid:
      return S_ERROR( "No certificate loaded" )

    if not limited:
      subj = self.__certObj.get_subject()
      lastEntry = subj.get_entry( subj.num_entries() - 1 )
      if lastEntry[0] == 'CN' and lastEntry[1] == "limited proxy":
        limited = True

    from DIRAC.Core.Security.X509Request import X509Request

    req = X509Request()
    req.generateProxyRequest( bitStrength = bitStrength, limited = limited )
    return S_OK( req )

  def getRemainingSecs( self ):
    """
    Get remaining lifetime in secs
    """
    if not self.__valid:
      return S_ERROR( "No certificate loaded" )
    notAfter = self.__certObj.get_not_after()
    remaining = notAfter - Time.dateTime()
    return S_OK( max( 0, remaining.days * 86400 + remaining.seconds ) )

  def getExtensions( self ):
    """
    Get a decoded list of extensions
    """
    if not self.__valid:
      return S_ERROR( "No certificate loaded" )
    extList = []
    for ext in self.__certObj.get_extensions():
      sn = ext.get_sn()
      try:
        value = ext.get_value()
      except Exception:
        value = "Cannot decode value"
      extList.append( ( sn, value ) )
    return S_OK( sorted( extList ) )
