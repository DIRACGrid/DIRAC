""" X509Certificate is a class for managing X509 certificates alone
"""
__RCSID__ = "$Id$"

import M2Crypto
import asn1
import datetime

import os
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities import Time
from DIRAC.Core.Utilities import DErrno
from DIRAC.ConfigurationSystem.Client.Helpers import Registry

VOMS_EXTENSION_OID = '1.3.6.1.4.1.8005.100.100.5'
VOMS_FQANS_OID = '1.3.6.1.4.1.8005.100.100.4'
VOMS_GENERIC_ATTRS_OID = '1.3.6.1.4.1.8005.100.100.11'
DOMAIN_COMPONENT_OID = '0.9.2342.19200300.100.1.25'
ORGANIZATIONAL_UNIT_NAME_OID = '2.5.4.11'
COMMON_NAME_OID = '2.5.4.3'

DN_MAPPING = {
    DOMAIN_COMPONENT_OID: '/DC=',
    ORGANIZATIONAL_UNIT_NAME_OID: '/OU=',
    COMMON_NAME_OID: '/CN='
}

class X509Certificate( object ):

  def __init__( self, x509Obj = None, certString = None ):
    """
    Constructor.

    :param x509Obj: (optional) certificate instance
    :type x509Obj: M2Crypto.X509.X509
    :param certString: text representation of certificate
    :type certString: String
    """
    self.__valid = False
    if x509Obj:
      self.__certObj = x509Obj
      self.__valid = True
    if certString:
      self.loadFromString( certString )

  def getCertObject( self ):
    return self.__certObj

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
      with file( certLocation ) as fd:
        pemData = fd.read()
    except IOError:
      return S_ERROR( DErrno.EOF, "Can't open %s file" % certLocation )

  def loadFromString( self, pemData ):
    """
    Load a x509 cert from a string containing the pem data
    Return : S_OK / S_ERROR
    """
    try:
      self.__certObj = GSI.crypto.load_certificate( GSI.crypto.FILETYPE_PEM, pemData )
    except Exception as e:
      return S_ERROR( DErrno.ECERTREAD, "Can't load pem data: %s" % e )
    self.__valid = True
    return S_OK()

  def setCertificate( self, x509Obj ):
    # XXX check if object is valid
    self.__certObj = x509Obj
    self.__valid = True
    return S_OK()

  def hasExpired( self ):
    """
    Check if a certificate file/proxy is still valid
    Return: S_OK( True/False )/S_ERROR
    """
    if not self.__valid:
      return S_ERROR( DErrno.ENOCERT )
    notAfter = self.__certObj.get_not_after().get_datetime()
    notAfter = notAfter.replace( tzinfo = Time.dateTime().tzinfo )
    return S_OK( notAfter < Time.dateTime() )

  def getNotAfterDate( self ):
    """
    Get not after date of a certificate
    Return: S_OK( datetime )/S_ERROR
    """
    if not self.__valid:
      return S_ERROR( DErrno.ENOCERT )
    return S_OK( self.__certObj.get_not_after() )

  def setNotAfter( self, notafter ):
    """
    Set not after date of a certificate
    Return: S_OK/S_ERROR
    """
    if not self.__valid:
      return S_ERROR( DErrno.ENOCERT )
    self.__certObj.set_not_after( notafter )
    return S_OK()

  def getNotBeforeDate( self ):
    """
    Get not before date of a certificate
    Return: S_OK( datetime )/S_ERROR
    """
    if not self.__valid:
      return S_ERROR( DErrno.ENOCERT )
    return S_OK( self.__certObj.get_not_before() )

  def setNotBefore( self, notbefore ):
    """
    Set not before date of a certificate
    Return: S_OK/S_ERROR
    """
    if not self.__valid:
      return S_ERROR( DErrno.ENOCERT )
    self.__certObj.set_not_before( notbefore )
    return S_OK()

  def getSubjectDN( self ):
    """
    Get subject DN
    Return: S_OK( string )/S_ERROR
    """
    if not self.__valid:
      return S_ERROR( DErrno.ENOCERT )
    return S_OK( str( self.__certObj.get_subject() ) )

  def getIssuerDN( self ):
    """
    Get issuer DN
    Return: S_OK( string )/S_ERROR
    """
    if not self.__valid:
      return S_ERROR( DErrno.ENOCERT )
    return S_OK( str(self.__certObj.get_issuer()) )

  def getSubjectNameObject( self ):
    """
    Get subject name object
    Return: S_OK( X509Name )/S_ERROR
    """
    if not self.__valid:
      return S_ERROR( DErrno.ENOCERT )
    return S_OK( self.__certObj.get_subject() )

  def getIssuerNameObject( self ):
    """
    Get issuer name object
    Return: S_OK( X509Name )/S_ERROR
    """
    if not self.__valid:
      return S_ERROR( DErrno.ENOCERT )
    return S_OK( self.__certObj.get_issuer() )

  def setIssuer( self, nameObject ):
    """
    Set issuer name object
    Return: S_OK/S_ERROR
    """
    if not self.__valid:
      return S_ERROR( DErrno.ENOCERT )
    self.__certObj.set_issuer( nameObject )
    return S_OK()

  def getPublicKey( self ):
    """
    Get the public key of the certificate
    """
    if not self.__valid:
      return S_ERROR( DErrno.ENOCERT )
    return S_OK( self.__certObj.get_pubkey() )

  def setPublicKey( self, pubkey ):
    """
    Set the public key of the certificate
    """
    if not self.__valid:
      return S_ERROR( DErrno.ENOCERT )
    self.__certObj.set_pubkey( pubkey )
    return S_OK()

  def getVersion( self ):
    """
    Get the version of the certificate
    """
    if not self.__valid:
      return S_ERROR(DErrno.ENOCERT)
    return S_OK(self.__certObj.get_version())

  def setVersion( self, version ):
    """
    Set the version of the certificate
    """
    if not self.__valid:
      return S_ERROR( DErrno.ENOCERT )
    self.__certObj.set_version( version )
    return S_OK()

  def getSerialNumber( self ):
    """
    Get certificate serial number
    Return: S_OK( serial )/S_ERROR
    """
    if not self.__valid:
      return S_ERROR( DErrno.ENOCERT )
    return S_OK( self.__certObj.get_serial_number() )

  def setSerialNumber( self, serial ):
    """
    Set certificate serial number
    Return: S_OK/S_ERROR
    """
    if self.__valid:
      self.__certObj.set_serial_number( serial )
      return S_OK()
    return S_ERROR( DErrno.ENOCERT )

  def sign( self, key, algo):
    """
    Sign the cerificate using provided key and algorithm.
    """
    if not self.__valid:
      return S_ERROR( DErrno.ENOCERT )
    self.__certObj.sign( key, algo )
    return S_OK()

  def getDIRACGroup( self, ignoreDefault = False ):
    """
    Get the dirac group if present
    """
    if not self.__valid:
      return S_ERROR( DErrno.ENOCERT )
    extCount = self.__certObj.get_ext_count()
    for extIdx in xrange(extCount):
      ext = self.__certObj.get_ext_at(extIdx)
      if ext.get_name() == "diracGroup":
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
      return S_ERROR( DErrno.ENOCERT )
    try:
      self.__certObj.get_ext('vomsExtensions')
      return S_OK( True )
    except:
      # no extension found
      pass
    return S_OK( False )

  def getVOMSData( self ):
    #return S_ERROR( DErrno.EVOMS, "No VOMS data available" )
    """
    Get voms extensions
    """
    decoder = asn1.Decoder()
    decoder.start(self.__certObj.as_der())
    data = parseForVOMS(decoder)
    if data:
      return S_OK(data)
    else:
      return S_ERROR( DErrno.EVOMS, "No VOMS data available" )


  def generateProxyRequest( self, bitStrength = 1024, limited = False ):
    """
    Generate a proxy request
    Return S_OK( X509Request ) / S_ERROR
    """
    if not self.__valid:
      return S_ERROR( DErrno.ENOCERT )

    if not limited:
      subj = self.__certObj.get_subject()
      lastEntry = subj[len(subj) - 1 ]
      if lastEntry.get_data() == "limited proxy":
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
      return S_ERROR( DErrno.ENOCERT )
    notAfter = self.__certObj.get_not_after().get_datetime()
    notAfter = notAfter.replace( tzinfo = Time.dateTime().tzinfo )
    remaining = notAfter - Time.dateTime()
    return S_OK( max( 0, remaining.days * 86400 + remaining.seconds ) )

  def getExtensions( self ):
    """
    Get a decoded list of extensions
    """
    if not self.__valid:
      return S_ERROR( DErrno.ENOCERT )
    extList = []
    for ext in self.__certObj.get_extensions():
      sn = ext.get_sn()
      try:
        value = ext.get_value()
      except Exception:
        value = "Cannot decode value"
      extList.append( ( sn, value ) )
    return S_OK( sorted( extList ) )
