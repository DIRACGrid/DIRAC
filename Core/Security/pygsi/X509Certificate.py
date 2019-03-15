""" X509Certificate is a class for managing X509 certificates alone
"""
__RCSID__ = "$Id$"

import GSI
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

  def __init__(self, x509Obj=None):
    self.__valid = False
    if x509Obj:
      self.__certObj = x509Obj
      self.__valid = True

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
      self.__certObj = GSI.crypto.load_certificate(GSI.crypto.FILETYPE_PEM, pemData)
    except Exception as e:
      return S_ERROR(DErrno.ECERTREAD, "Can't load pem data: %s" % e)
    self.__valid = True
    return S_OK()

  def setCertificate(self, x509Obj):
    if not isinstance(x509Obj, GSI.crypto.X509Type):
      return S_ERROR(DErrno.ETYPE, "Object %s has to be of type X509" % str(x509Obj))
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
    return S_OK(self.__certObj.has_expired())

  def getNotAfterDate(self):
    """
    Get not after date of a certificate
    Return: S_OK( datetime )/S_ERROR
    """
    if not self.__valid:
      return S_ERROR(DErrno.ENOCERT)
    return S_OK(self.__certObj.get_not_after())

  def getNotBeforeDate(self):
    """
    Get not before date of a certificate
    Return: S_OK( datetime )/S_ERROR
    """
    if not self.__valid:
      return S_ERROR(DErrno.ENOCERT)
    return S_OK(self.__certObj.get_not_before())

  def getSubjectDN(self):
    """
    Get subject DN
    Return: S_OK( string )/S_ERROR
    """
    if not self.__valid:
      return S_ERROR(DErrno.ENOCERT)
    return S_OK(self.__certObj.get_subject().one_line())

  def getIssuerDN(self):
    """
    Get issuer DN
    Return: S_OK( string )/S_ERROR
    """
    if not self.__valid:
      return S_ERROR(DErrno.ENOCERT)
    return S_OK(self.__certObj.get_issuer().one_line())

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

  def getPublicKey(self):
    """
    Get the public key of the certificate
    """
    if not self.__valid:
      return S_ERROR(DErrno.ENOCERT)
    return S_OK(self.__certObj.get_pubkey())

  def getSerialNumber(self):
    """
    Get certificate serial number
    Return: S_OK( serial )/S_ERROR

    :warn:  This does not work. It sends rubish binary data.
    """
    if not self.__valid:
      return S_ERROR(DErrno.ENOCERT)
    return S_OK(self.__certObj.get_serial_number())

  def getDIRACGroup(self, ignoreDefault=False):
    """
    Get the dirac group if present
    """
    if not self.__valid:
      return S_ERROR(DErrno.ENOCERT)
    extList = self.__certObj.get_extensions()
    for ext in extList:
      if ext.get_sn() == "diracGroup":
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
    extList = self.__certObj.get_extensions()
    for ext in extList:
      if ext.get_sn() == "vomsExtensions":
        return S_OK(True)
    return S_OK(False)

  def getVOMSData(self):
    """
    Has voms extensions
    """
    if not self.__valid:
      return S_ERROR(DErrno.ENOCERT)
    extList = self.__certObj.get_extensions()
    for ext in extList:
      if ext.get_sn() == "vomsExtensions":
        data = {}
        raw = ext.get_asn1_value().get_value()
        name = self.__certObj.get_subject().clone()
        while name.num_entries() > 0:
          name.remove_entry(0)
        for entry in raw[0][0][0][1][0][0][0][0]:
          name.insert_entry(entry[0][0], entry[0][1])
        data['subject'] = name.one_line()
        while name.num_entries() > 0:
          name.remove_entry(0)
        for entry in raw[0][0][0][2][0][0][0]:
          name.insert_entry(entry[0][0], entry[0][1])
        data['issuer'] = name.one_line()
        data['notBefore'] = raw[0][0][0][5][0]
        data['notAfter'] = raw[0][0][0][5][1]
        data['fqan'] = [str(fqan) for fqan in raw[0][0][0][6][0][1][0][1]]
        for extBundle in raw[0][0][0][7]:
          if extBundle[0] == "VOMS attribute":
            attr = GSI.crypto.asn1_loads(str(extBundle[1])).get_value()
            attr = attr[0][0][1][0]
            try:
              data['attribute'] = "%s = %s (%s)" % attr
              data['vo'] = attr[2]
            except Exception as _ex:
              data['attribute'] = "Cannot decode VOMS attribute"
        if 'vo' not in data and 'fqan' in data:
          data['vo'] = data['fqan'][0].split('/')[1]
        return S_OK(data)
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
      lastEntry = subj.get_entry(subj.num_entries() - 1)
      if lastEntry[0] == 'CN' and lastEntry[1] == "limited proxy":
        limited = True

    from DIRAC.Core.Security.pygsi.X509Request import X509Request  # pylint: disable=import-error

    req = X509Request()
    req.generateProxyRequest(bitStrength=bitStrength, limited=limited)
    return S_OK(req)

  def getRemainingSecs(self):
    """
    Get remaining lifetime in secs
    """
    if not self.__valid:
      return S_ERROR(DErrno.ENOCERT)
    notAfter = self.__certObj.get_not_after()
    remaining = notAfter - Time.dateTime()
    return S_OK(max(0, remaining.days * 86400 + remaining.seconds))

  def getExtensions(self):
    """
    Get a decoded list of extensions
    """
    if not self.__valid:
      return S_ERROR(DErrno.ENOCERT)
    extList = []
    for ext in self.__certObj.get_extensions():
      sn = ext.get_sn()
      try:
        value = ext.get_value()
      except Exception:
        value = "Cannot decode value"
      extList.append((sn, value))
    return S_OK(sorted(extList))
