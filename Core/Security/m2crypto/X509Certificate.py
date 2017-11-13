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

# List of OIDs used in handling VOMS extension.
# VOMS extension is encoded in ASN.1 format and it's surprisingly hard to decode. OIDs describe content of sections
# of the data. There is no "official list of OIDs", ones used here are sourced from analysing VOMS extensions itself
# and different pieces of code and presentations in subject of X509 certificates, certificate extensions and VOMS.
# Googling names or values of those OIDs, especially VOMS related, usually result in up to three pages of results,
# mainly Java code defining those values like code below.
# This is literally lookup table, so I know WTH is this, when I read value and see '1.3.6.1.4.1.8005.100.100.4'.
VOMS_EXTENSION_OID = '1.3.6.1.4.1.8005.100.100.5'
VOMS_FQANS_OID = '1.3.6.1.4.1.8005.100.100.4'
VOMS_GENERIC_ATTRS_OID = '1.3.6.1.4.1.8005.100.100.11'
DOMAIN_COMPONENT_OID = '0.9.2342.19200300.100.1.25'
ORGANIZATIONAL_UNIT_NAME_OID = '2.5.4.11'
COMMON_NAME_OID = '2.5.4.3'
LIMITED_PROXY_OID = '1.3.6.1.4.1.3536.1.1.1.9'

DN_MAPPING = {
    DOMAIN_COMPONENT_OID: '/DC=',
    ORGANIZATIONAL_UNIT_NAME_OID: '/OU=',
    COMMON_NAME_OID: '/CN='
}


class X509Certificate(object):

  def __init__(self, x509Obj=None, certString=None):
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
    else:
      self.__certObj = M2Crypto.X509.X509()
      self.__valid = True
    if certString:
      self.loadFromString(certString)

  def getCertObject(self):
    return self.__certObj

  def load(self, certificate):
    """ Load a x509 certificate either from a file or from a string
    """

    if os.path.exists(certificate):
      return self.loadFromFile(certificate)
    else:
      return self.loadFromString(certificate)

  def loadFromFile(self, certLocation):
    """
    Load a x509 cert from a pem file
    Return : S_OK / S_ERROR
    """
    try:
      with open(certLocation, 'r') as fd:
        pemData = fd.read()
        return self.loadFromString(pemData)
    except IOError:
      return S_ERROR(DErrno.EOF, "Can't open %s file" % certLocation)

  def loadFromString(self, pemData):
    """
    Load a x509 cert from a string containing the pem data
    Return : S_OK / S_ERROR
    """
    try:
      self.__certObj = M2Crypto.X509.load_cert_string(str(pemData), M2Crypto.X509.FORMAT_PEM)
    except Exception, e:
      return S_ERROR(DErrno.ECERTREAD, "Can't load pem data: %s" % e)
    self.__valid = True
    return S_OK()

  def setCertificate(self, x509Obj):
    """
    Set certificate object
    Return: S_OK/S_ERROR
    """
    if not isinstance(x509Obj, M2Crypto.X509.X509):
      return S_ERROR(DErrno.ETYPE, "Object %s has to be of type M2Crypto.X509.X509" % str(x509Obj))
    self.__certObj = x509Obj
    self.__valid = True
    return S_OK()

  def hasExpired(self):
    """
    Check if a certificate file/proxy is still valid
    Return: S_OK( True/False )/S_ERROR
    """
    if not self.__valid:
      return S_ERROR(DErrno.ENOCERT)
    notAfter = self.__certObj.get_not_after().get_datetime()
    notAfter = notAfter.replace(tzinfo=Time.dateTime().tzinfo)
    return S_OK(notAfter < Time.dateTime())

  def getNotAfterDate(self):
    """
    Get not after date of a certificate
    Return: S_OK( datetime )/S_ERROR
    """
    if not self.__valid:
      return S_ERROR(DErrno.ENOCERT)
    return S_OK(self.__certObj.get_not_after())

  def setNotAfter(self, notafter):
    """
    Set not after date of a certificate
    Return: S_OK/S_ERROR
    """
    if not self.__valid:
      return S_ERROR(DErrno.ENOCERT)
    self.__certObj.set_not_after(notafter)
    return S_OK()

  def getNotBeforeDate(self):
    """
    Get not before date of a certificate
    Return: S_OK( datetime )/S_ERROR
    """
    if not self.__valid:
      return S_ERROR(DErrno.ENOCERT)
    return S_OK(self.__certObj.get_not_before())

  def setNotBefore(self, notbefore):
    """
    Set not before date of a certificate
    Return: S_OK/S_ERROR
    """
    if not self.__valid:
      return S_ERROR(DErrno.ENOCERT)
    self.__certObj.set_not_before(notbefore)
    return S_OK()

  def getSubjectDN(self):
    """
    Get subject DN
    Return: S_OK( string )/S_ERROR
    """
    if not self.__valid:
      return S_ERROR(DErrno.ENOCERT)
    return S_OK(str(self.__certObj.get_subject()))

  def getIssuerDN(self):
    """
    Get issuer DN
    Return: S_OK( string )/S_ERROR
    """
    if not self.__valid:
      return S_ERROR(DErrno.ENOCERT)
    return S_OK(str(self.__certObj.get_issuer()))

  def getSubjectNameObject(self):
    """
    Get subject name object
    Return: S_OK( X509Name )/S_ERROR
    """
    if not self.__valid:
      return S_ERROR(DErrno.ENOCERT)
    return S_OK(self.__certObj.get_subject())

  def getIssuerNameObject(self):
    """
    Get issuer name object
    Return: S_OK( X509Name )/S_ERROR
    """
    if not self.__valid:
      return S_ERROR(DErrno.ENOCERT)
    return S_OK(self.__certObj.get_issuer())

  def setIssuer(self, nameObject):
    """
    Set issuer name object
    Return: S_OK/S_ERROR
    """
    if not self.__valid:
      return S_ERROR(DErrno.ENOCERT)
    self.__certObj.set_issuer(nameObject)
    return S_OK()

  def getPublicKey(self):
    """
    Get the public key of the certificate
    """
    if not self.__valid:
      return S_ERROR(DErrno.ENOCERT)
    return S_OK(self.__certObj.get_pubkey())

  def setPublicKey(self, pubkey):
    """
    Set the public key of the certificate
    """
    if not self.__valid:
      return S_ERROR(DErrno.ENOCERT)
    self.__certObj.set_pubkey(pubkey)
    return S_OK()

  def getVersion(self):
    """
    Get the version of the certificate
    """
    if not self.__valid:
      return S_ERROR(DErrno.ENOCERT)
    return S_OK(self.__certObj.get_version())

  def setVersion(self, version):
    """
    Set the version of the certificate
    """
    if not self.__valid:
      return S_ERROR(DErrno.ENOCERT)
    self.__certObj.set_version(version)
    return S_OK()

  def getSerialNumber(self):
    """
    Get certificate serial number
    Return: S_OK( serial )/S_ERROR
    """
    if not self.__valid:
      return S_ERROR(DErrno.ENOCERT)
    return S_OK(self.__certObj.get_serial_number())

  def setSerialNumber(self, serial):
    """
    Set certificate serial number
    Return: S_OK/S_ERROR
    """
    if self.__valid:
      self.__certObj.set_serial_number(serial)
      return S_OK()
    return S_ERROR(DErrno.ENOCERT)

  def sign(self, key, algo):
    """
    Sign the cerificate using provided key and algorithm.
    """
    if not self.__valid:
      return S_ERROR(DErrno.ENOCERT)
    self.__certObj.sign(key, algo)
    return S_OK()

  def getDIRACGroup(self, ignoreDefault=False):
    """
    Get the dirac group if present
    """
    if not self.__valid:
      return S_ERROR(DErrno.ENOCERT)
    extCount = self.__certObj.get_ext_count()
    for extIdx in xrange(extCount):
      ext = self.__certObj.get_ext_at(extIdx)
      if ext.get_name() == "diracGroup":
        return S_OK(ext.get_value())
    if ignoreDefault:
      return S_OK(False)
    result = self.getIssuerDN()
    if not result['OK']:
      return result
    return Registry.findDefaultGroupForDN(result['Value'])

  def hasVOMSExtensions(self):
    """
    Has voms extensions
    """
    if not self.__valid:
      return S_ERROR(DErrno.ENOCERT)
    try:
      self.__certObj.get_ext('vomsExtensions')
      return S_OK(True)
    except LookupError:
      # no extension found
      pass
    return S_OK(False)

  def getVOMSData(self):
    # return S_ERROR( DErrno.EVOMS, "No VOMS data available" )
    """
    Get voms extensions
    """
    decoder = asn1.Decoder()
    decoder.start(self.__certObj.as_der())
    data = parseForVOMS(decoder)
    if data:
      return S_OK(data)
    else:
      return S_ERROR(DErrno.EVOMS, "No VOMS data available")

  def generateProxyRequest(self, bitStrength=1024, limited=False):
    """
    Generate a proxy request
    Return S_OK( X509Request ) / S_ERROR
    """
    if not self.__valid:
      return S_ERROR(DErrno.ENOCERT)

    if not limited:
      subj = self.__certObj.get_subject()
      lastEntry = subj[len(subj) - 1]
      if lastEntry.get_data() == "limited proxy":
        limited = True

    from DIRAC.Core.Security.m2crypto.X509Request import X509Request

    req = X509Request()
    req.generateProxyRequest(bitStrength=bitStrength, limited=limited)
    return S_OK(req)

  def getRemainingSecs(self):
    """
    Get remaining lifetime in secs
    """
    if not self.__valid:
      return S_ERROR(DErrno.ENOCERT)
    notAfter = self.__certObj.get_not_after().get_datetime()
    notAfter = notAfter.replace(tzinfo=Time.dateTime().tzinfo)
    remaining = notAfter - Time.dateTime()
    return S_OK(max(0, remaining.days * 86400 + remaining.seconds))

  def getExtensions(self):
    """
    Get a decoded list of extensions
    """
    if not self.__valid:
      return S_ERROR(DErrno.ENOCERT)
    extList = []
    for i in self.__certObj.get_ext_count():
      sn = self.__certObj.get_ext_at(i).get_name()
      try:
        value = self.__certObj.get_ext_at(i).get_value()
      except Exception:
        value = "Cannot decode value"
      extList.append((sn, value))
    return S_OK(sorted(extList))

  def verify(self, pkey):
    """
    Verify certificate using provided key
    """
    ret = self.__certObj.verify(pkey)
    return S_OK(ret)

  def setSubject(self, subject):
    """
    Set subject using provided X509Name object
    """
    if not self.__valid:
      return S_ERROR(DErrno.ENOCERT)
    self.__certObj.set_subject(subject)
    return S_OK()

  def get_subject(self):
    """
    Deprecated way of getting subject DN. Only for backward compatibility reasons.
    """
    # XXX This function should be deleted when all code depending on it is updated.
    return self.getSubjectDN()['Value']  # XXX FIXME awful awful hack

  def asPem(self):
    """
    Return cerificate as PEM string
    """
    return self.__certObj.as_pem()

  def getExtension(self, name):
    """
    Return X509 Extension with given name
    """
    try:
      ext = self.__certObj.get_ext(name)
    except LookupError as LE:
      return S_ERROR(LE)
    return S_OK(ext)

  def addExtension(self, extension):
    """
    Add extension to the certificate
    """
    if not self.__valid:
      return S_ERROR(DErrno.ENOCERT)
    self.__certObj.add_ext(extension)
    return S_OK()

# utility functions for handling VOMS Extension


def extract_DN(inp):
  """
  Return DN extracted from given ASN.1 decoder
  """
  while not inp.peek().nr == asn1.Numbers.Set:  # looking for the sequence of sets, so if set is next, we're here
    inp = enterSequence(inp)
  dn = ""
  while inp.peek():  # each set has OID and value
    inp.enter()
    inp.enter()
    _, oid = inp.read()
    _, value = inp.read()
    dn += DN_MAPPING[oid]
    dn += value
    inp.leave()
    inp.leave()
  return dn


def enterSequence(seq, levels=1):
  """
  Enter sequence in ASN.1 decoder
  """
  while levels:
    tag = seq.peek()
    if not tag.typ == asn1.Types.Constructed:
      return seq
    seq.enter()
    levels -= 1
  return seq


def leaveSequence(inp):
  """
  Leave sequence in ASN.1 decoder. Leaves all nested sequences if there are no more values to read
  """
  while not inp.peek():
    inp.leave()
  return inp


def parseForVOMS(inp):
  """
  Parse ASN.1 encoded X509 Extension. Recursively looks for VOMS extension.
  """
  while not inp.eof():
    tag = inp.peek()
    if tag.typ == asn1.Types.Primitive:
      tag, value = inp.read()
      if tag.nr == asn1.Numbers.ObjectIdentifier and value == VOMS_EXTENSION_OID:  # we have our voms
        voms_decoder = asn1.Decoder()
        _, value = inp.read()
        voms_decoder.start(value)
        data = processVOMSExtension(voms_decoder)
        return data
      else:
        pass
        # we don't care about other extensions, but I wanted to make it clear, that's why "else: pass", sorry not sorry
    elif tag.typ == asn1.Types.Constructed:
      inp.enter()
      data = parseForVOMS(inp)
      if data:
        return data
      inp.leave()


def processVOMSExtension(inp):
  """
  Extact VOMS information from ASN.1 decoder containing VOMS extension
  """
  data = {}

  # we have sequential access, no random access, only way to advance is to read
  while inp.peek().nr == asn1.Numbers.Sequence:
    inp = enterSequence(inp)  # get one level deeper
  inp.read()  # skippinng, this is not value we are looking for
  data['subject'] = extract_DN(inp)

  # jump back to level, where there is something to read
  inp = leaveSequence(inp)
  inp.read()  # there is one more value in this sequence, but we don't care
  inp = leaveSequence(inp)
  data['issuer'] = extract_DN(inp)

  inp = leaveSequence(inp)
  # skipping two fields
  inp.read()
  inp.read()

  inp.enter()
  _, notBefore = inp.read()
  _, notAfter = inp.read()
  data['notBefore'] = datetime.datetime.strptime(notBefore[:-1], '%Y%m%d%H%M%S')
  data['notAfter'] = datetime.datetime.strptime(notAfter[:-1], '%Y%m%d%H%M%S')

  fqan = []
  inp = leaveSequence(inp)

  while not inp.peek().nr == asn1.Numbers.ObjectIdentifier:
    inp = enterSequence(inp)

  if inp.peek().nr == asn1.Numbers.ObjectIdentifier:
    _, value = inp.read()
    if value == VOMS_FQANS_OID:
      while inp.peek().nr == asn1.Numbers.Set:
        inp = enterSequence(inp)
      inp = enterSequence(inp)
      inp.read()  # skipping
      inp = enterSequence(inp)
      _, value = inp.read()
      fqan.append(value.decode('utf-8'))
      _, value = inp.read()
      fqan.append(value.decode('utf-8'))
      data['fqan'] = fqan

  inp = leaveSequence(inp)
  inp = enterSequence(inp, 2)
  _, value = inp.read()
  if value == VOMS_GENERIC_ATTRS_OID:
    dec = asn1.Decoder()
    _, vv = inp.read()
    dec.start(vv)
    dec = enterSequence(dec, 3)
    dec.read()  # skipping
    dec = enterSequence(dec, 2)
    _, name = dec.read()
    _, value = dec.read()
    _, aux = dec.read()

    data['attribute'] = "%s = %s (%s)" % (name.decode('utf-8'), value.decode('utf-8'), aux.decode('utf-8'))
    data['vo'] = aux.decode('utf-8')

  if 'vo' not in data and 'fqan' in data:
    data['vo'] = fqan[0].split('/')[1]
  return data
