""" X509CRL is a class for managing X509CRL
This class is used to manage the revoked certificates....
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
__RCSID__ = "$Id$"


import stat
import os
import tempfile
import re
import datetime

import M2Crypto
import six
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities import DErrno

# pylint: disable=broad-except


class X509CRL(object):

  def __init__(self, cert=None):

    self.__pemData = None

    if cert:
      self.__loadedCert = True
      self.__revokedCert = cert
    else:
      self.__loadedCert = False

  @classmethod
  def instanceFromFile(cls, crlLocation):
    """ Instance a X509CRL from a file
    """
    crl = cls()
    result = crl.loadCRLFromFile(crlLocation)
    if not result['OK']:
      return result
    return S_OK(crl)

  def loadCRLFromFile(self, crlLocation):
    """
    Load a x509CRL certificate from a pem file
    Return : S_OK / S_ERROR
    """

    self.__loadedCert = False
    try:
      self.__revokedCert = M2Crypto.X509.load_crl(crlLocation)
    except Exception as e:
      return S_ERROR(DErrno.ECERTREAD, "%s" % repr(e).replace(',)', ')'))
    self.__loadedCert = True
    with open(crlLocation, 'rb') as crlFile:
      pemData = crlFile.read()
    self.__pemData = pemData
    return S_OK()

  def __bytes__(self):
    if not self.__loadedCert:
      return b"No certificate loaded"
    return self.__pemData

  if six.PY2:
    def __str__(self):
      return self.__bytes__()
  else:
    def __str__(self):
      return bytes(self).decode()

  def dumpAllToString(self):
    """
    Dump all to string
    """
    if not self.__loadedCert:
      return S_ERROR(DErrno.ECERTREAD, "No certificate loaded")

    return S_OK(self.__pemData)

  def dumpAllToFile(self, filename=False):
    """
    Dump all to file. If no filename specified a temporal one will be created
    """
    if not self.__loadedCert:
      return S_ERROR("No certificate loaded")
    try:
      if filename:
        with open(filename, "wb") as fp:
          fp.write(bytes(self))
      else:
        with tempfile.NamedTemporaryFile("wb", delete=False) as fp:
          filename = fp.name
          fp.write(bytes(self))
    except Exception as e:
      return S_ERROR(DErrno.EWF, "%s: %s" % (filename, repr(e).replace(',)', ')')))
    try:
      os.chmod(filename, stat.S_IRUSR | stat.S_IWUSR)
    except Exception as e:
      return S_ERROR(DErrno.ESPF, "%s: %s" % (filename, repr(e).replace(',)', ')')))
    return S_OK(filename)

  def hasExpired(self):
    if not self.__loadedCert:
      return S_ERROR("No certificate loaded")
    # XXX It should be done better, for now M2Crypto doesn't offer access to fields like Next Update
    txt = self.__revokedCert.as_text()
    pattern = r"Next Update: (?P<nextUpdate>.*)\n"
    dateStr = re.search(pattern.encode("utf-8"), txt).group('nextUpdate')
    nextUpdate = datetime.datetime.strptime(dateStr, "%b %d %H:%M:%S %Y GMT")
    return S_OK(datetime.datetime.now() > nextUpdate)

  def getIssuer(self):
    if not self.__loadedCert:
      return S_ERROR("No certificate loaded")
    # XXX It sould be done better, for now M2Crypto doesn't offer access to fields like Issuer
    txt = self.__revokedCert.as_text()
    pattern = r"Issuer: (?P<issuer>.*)\n"
    return S_OK(re.search(pattern.encode("utf-8"), txt).group('issuer'))

  def __repr__(self):
    repStr = "<X509CRL"
    if self.__loadedCert:
      repStr += ""  # self.__revokedCert.get_issuer().one_line()  # Why issuer?! XXX
    repStr += ">"
    return repStr
