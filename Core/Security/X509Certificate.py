""" X509Certificate is a class for managing X509 certificates alone
"""
__RCSID__ = "$Id$"

import M2Crypto
import datetime

import os
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities import Time
from DIRAC.Core.Utilities import DErrno
from DIRAC.ConfigurationSystem.Client.Helpers import Registry

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
    return self.loadFromString( pemData )

  def loadFromString( self, pemData ):
    """
    Load a x509 cert from a string containing the pem data
    Return : S_OK / S_ERROR
    """
    try:
      self.__certObj = M2Crypto.X509.load_cert_string( pemData, M2Crypto.X509.FORMAT_PEM )
    except Exception, e:
      return S_ERROR( DErrno.ECERTREAD, "Can't load pem data: %s" % e )
    self.__valid = True
    return S_OK()

  def setCertificate( self, x509Obj ):
    if not isinstance( x509Obj, M2Crypto.X509.X509 ):
      return S_ERROR( DErrno.ETYPE, "Object %s has to be of type M2Crypto.X509.X509" % str( x509Obj ) )
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

  def getNotBeforeDate( self ):
    """
    Get not before date of a certificate
    Return: S_OK( datetime )/S_ERROR
    """
    if not self.__valid:
      return S_ERROR( DErrno.ENOCERT )
    return S_OK( self.__certObj.get_not_before() )

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

  def getPublicKey( self ):
    """
    Get the public key of the certificate
    """
    if not self.__valid:
      return S_ERROR( DErrno.ENOCERT )
    return S_OK( self.__certObj.get_pubkey() )

  def getVersion( self ):
    if not self.__valid:
      return S_ERROR(DErrno.ENOCERT)
    return S_OK(self.__certObj.get_version())

  def getSerialNumber( self ):
    """
    Get certificate serial number
    Return: S_OK( serial )/S_ERROR
    """
    if not self.__valid:
      return S_ERROR( DErrno.ENOCERT )
    return S_OK( self.__certObj.get_serial_number() )

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
      extList = self.__certObj.get_ext('vomsExtensions')
      return S_OK( True )
    except:
      # no extension found
      pass
    return S_OK( False )

  def getVOMSData( self ):
    """
    Get voms extensions
    """
    # XXX Temporary "fix". There are issues with reading vomsExtensions using M2Crypto and this seems to be used only for displaying info.
    data = {}
    data[ 'issuer' ] = 'fake'
    data[ 'notBefore' ] = datetime.datetime.now()
    data[ 'notAfter' ] = datetime.datetime.now()
    data[ 'fqan' ] = 'fake'
    data[ 'attribute' ] = 'fake'
    data[ 'vo' ] = 'fake'
    data[ 'subject' ] = 'fake'
    return S_OK(data)

    # XXX This is HIDEOUS and totally unreadable. Will be rewritten.
    if not self.__valid:
      return S_ERROR( DErrno.ENOCERT )
    extCount = self.__certObj.get_ext_count()
    for extIdx in xrange(extCount):
      ext = self.__certObj.get_ext_at(extIdx)
      if ext.get_name() == "vomsExtensions":
        for i in xrange(65535):
          try:
            print ext.get_value(flag=i)
          except:
            pass
        print '>>>>>>', ext.get_value()
        data = {}
        raw = ext.get_asn1_value().get_value()
        name = self.__certObj.get_subject().clone()
        while name.num_entries() > 0:
          name.remove_entry( 0 )
        for entry in raw[0][0][0][1][0][0][0][0]:
          name.insert_entry( entry[0][0], entry[0][1] )
        data[ 'subject' ] = name.one_line()
        while name.num_entries() > 0:
          name.remove_entry( 0 )
        for entry in raw[0][0][0][2][0][0][0]:
          name.insert_entry( entry[0][0], entry[0][1] )
        data[ 'issuer' ] = name.one_line()
        data[ 'notBefore' ] = raw[0][0][0][5][0]
        data[ 'notAfter' ] = raw[0][0][0][5][1]
        data[ 'fqan' ] = [ str(fqan) for fqan in raw[0][0][0][6][0][1][0][1] ]
        for extBundle in raw[0][0][0][7]:
          if extBundle[0] == "VOMS attribute":
            attr = GSI.crypto.asn1_loads( str(extBundle[1]) ).get_value()
            attr = attr[0][0][1][0]
            try:
              data[ 'attribute' ] = "%s = %s (%s)" % attr
              data[ 'vo' ] = attr[2]
            except Exception as _ex:
              data[ 'attribute' ] = "Cannot decode VOMS attribute"
        if not 'vo' in data and 'fqan' in data:
          data['vo'] = data['fqan'][0].split( '/' )[1]
        return S_OK( data )
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

  def verify( self, pkey ):
    ret = self.__certObj.verify( pkey )
    return S_OK( ret )

  def get_subject( self ):
    # XXX This function should be deleted when all code depending on it is updated.
    return self.getSubjectDN()['Value'] # XXX FIXME awful awful hack

  def asPem( self ):
    return self.__certObj.as_pem()

  def getExtension( self, name ):
    try:
      ext = self.__certObj.get_ext( name )
    except LookupError as LE:
      return S_ERROR( LE )
    return S_OK( ext )
