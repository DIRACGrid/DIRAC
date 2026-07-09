""" X509Certificate is a class for managing X509 certificates

Proxy RFC: https://tools.ietf.org/html/rfc3820

X509 RFC: https://tools.ietf.org/html/rfc5280

"""
import datetime
import os
import secrets

from cryptography import x509
from cryptography.hazmat import asn1 as _asn1
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec, rsa

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities import DErrno
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.Core.Security import DEFAULT_PROXY_STRENGTH, DIRAC_GROUP_OID, PROXY_CERT_INFO_EXTENSION_OID
from DIRAC.Core.Security import asn1_utils
from DIRAC.Core.Utilities.Decorators import executeOnlyIf

# Decorator to execute the method only of the certificate has been loaded
executeOnlyIfCertLoaded = executeOnlyIf("_certLoaded", S_ERROR(DErrno.ENOCERT))


# Mapping from extension OID to the OpenSSL short name, as returned by
# M2Crypto's X509_Extension.get_name. Extensions not listed here show
# up as UNDEF, like they used to.
_EXT_OID_TO_NAME = {
    "2.5.29.14": "subjectKeyIdentifier",
    "2.5.29.15": "keyUsage",
    "2.5.29.17": "subjectAltName",
    "2.5.29.18": "issuerAltName",
    "2.5.29.19": "basicConstraints",
    "2.5.29.31": "crlDistributionPoints",
    "2.5.29.32": "certificatePolicies",
    "2.5.29.35": "authorityKeyIdentifier",
    "2.5.29.37": "extendedKeyUsage",
    "1.3.6.1.5.5.7.1.1": "authorityInfoAccess",
    "2.16.840.1.113730.1.1": "nsCertType",
    "2.16.840.1.113730.1.13": "nsComment",
    PROXY_CERT_INFO_EXTENSION_OID: "proxyCertInfo",
}

# OpenSSL display names for the extended key usages
_EKU_TO_NAME = {
    "1.3.6.1.5.5.7.3.1": "TLS Web Server Authentication",
    "1.3.6.1.5.5.7.3.2": "TLS Web Client Authentication",
    "1.3.6.1.5.5.7.3.3": "Code Signing",
    "1.3.6.1.5.5.7.3.4": "E-mail Protection",
    "1.3.6.1.5.5.7.3.8": "Time Stamping",
    "1.3.6.1.5.5.7.3.9": "OCSP Signing",
    "2.5.29.37.0": "Any Extended Key Usage",
}

# OpenSSL display names for the key usages, in the order OpenSSL prints them
_KU_FLAGS = (
    ("digital_signature", "Digital Signature"),
    ("content_commitment", "Non Repudiation"),
    ("key_encipherment", "Key Encipherment"),
    ("data_encipherment", "Data Encipherment"),
    ("key_agreement", "Key Agreement"),
    ("key_cert_sign", "Certificate Sign"),
    ("crl_sign", "CRL Sign"),
    ("encipher_only", "Encipher Only"),
    ("decipher_only", "Decipher Only"),
)


def _formatGeneralName(gn):
    """Format a cryptography GeneralName the way OpenSSL prints them"""
    if isinstance(gn, x509.DNSName):
        return f"DNS:{gn.value}"
    if isinstance(gn, x509.IPAddress):
        return f"IP Address:{gn.value}"
    if isinstance(gn, x509.RFC822Name):
        return f"email:{gn.value}"
    if isinstance(gn, x509.UniformResourceIdentifier):
        return f"URI:{gn.value}"
    if isinstance(gn, x509.DirectoryName):
        return f"DirName:{asn1_utils.nameToDN(gn.value)}"
    if isinstance(gn, x509.RegisteredID):
        return f"Registered ID:{gn.value.dotted_string}"
    return str(gn)


def _formatExtensionValue(extValue):
    """Return a text rendering of an extension value, mimicking what OpenSSL
    X509V3_EXT_print (and thus M2Crypto's X509_Extension.get_value) produces
    for the extension types DIRAC cares about.
    """
    if isinstance(extValue, x509.BasicConstraints):
        text = f"CA:{str(extValue.ca).upper()}"
        if extValue.path_length is not None:
            text += f", pathlen:{extValue.path_length}"
        return text
    if isinstance(extValue, x509.KeyUsage):
        flags = []
        for attr, name in _KU_FLAGS:
            try:
                if getattr(extValue, attr):
                    flags.append(name)
            except ValueError:
                # encipher_only/decipher_only are undefined without key_agreement
                pass
        return ", ".join(flags)
    if isinstance(extValue, x509.ExtendedKeyUsage):
        return ", ".join(_EKU_TO_NAME.get(oid.dotted_string, oid.dotted_string) for oid in extValue)
    if isinstance(extValue, (x509.SubjectAlternativeName, x509.IssuerAlternativeName)):
        return ", ".join(_formatGeneralName(gn) for gn in extValue)
    if isinstance(extValue, x509.SubjectKeyIdentifier):
        return ":".join(f"{c:02X}" for c in extValue.digest)
    if isinstance(extValue, x509.AuthorityKeyIdentifier):
        if extValue.key_identifier:
            return "keyid:" + ":".join(f"{c:02X}" for c in extValue.key_identifier)
        return ""
    if isinstance(extValue, x509.UnrecognizedExtension):
        oid = extValue.oid.dotted_string
        if oid in (DIRAC_GROUP_OID, "2.16.840.1.113730.1.13"):
            # Both the diracGroup and nsComment extensions contain an IA5String
            return _asn1.decode_der(_asn1.IA5String, extValue.public_bytes()).as_str()
        if oid == PROXY_CERT_INFO_EXTENSION_OID:
            proxyCertInfo = _asn1.decode_der(asn1_utils.ProxyCertInfo, extValue.public_bytes())
            pathLen = proxyCertInfo.pCPathLenConstraint
            return (
                f"Path Length Constraint: {'infinite' if pathLen is None else pathLen}\n"
                f"Policy Language: {proxyCertInfo.proxyPolicy.policyLanguage.dotted_string}"
            )
    raise ValueError(f"No text representation for {extValue!r}")


class X509Certificate:
    """The X509Certificate object represents ... a X509Certificate.

    It is a wrapper around a lower level implementation (pyca/cryptography) of a certificate.
    In theory, it can be a host or user certificate. Also, a proxy certificate is a X509Certificate,
    however it is useless without all the chain of issuers.
    That's why one has the X509Chain.

    Note that the SSL connection itself does not use this class, it gives directly the certificate to the library
    """

    def __init__(self, x509Obj=None, certString=None):
        """
        Constructor.
        You can give either nothing, or the x509Obj or the certString

        :param x509Obj: (optional) certificate instance
        :type x509Obj: cryptography.x509.Certificate
        :param certString: text representation of certificate
        :type certString: String

        """

        self._certLoaded = False
        self._certObj = None
        if x509Obj is not None:
            self._certObj = x509Obj
            self._certLoaded = True
        elif certString:
            self.loadFromString(certString)

    @classmethod
    def generateProxyCertFromIssuer(cls, x509Issuer, extensions, proxyPublicKey, signingKey, lifetime=3600):
        """This class method is meant to generate a new X509Certificate out of an existing one.
        Basically, it generates a proxy... However, you can't have a proxy certificate working on
        its own, you need all the chain of certificates. This method is meant to be called
        only by the X509Chain class.

        :param x509Issuer: X509Certificate instance from which we generate the next one
        :param extensions: list of (cryptography.x509.ExtensionType, critical) to add to the new certificate.
                           It contains all the X509 extensions needed for the proxy (e.g. DIRAC group).
                           See ~X509Chain.__getProxyExtensionList
        :param proxyPublicKey: cryptography public key object to certify
        :param signingKey: cryptography private key object of the issuer, used for signing
        :param lifetime: duration of the proxy in second. Default 3600

        :returns: S_OK(X509Certificate), the new (signed) proxy certificate

        """
        # According to the proxy RFC, the serial number just needs to be unique
        # among the proxy generated by the issuer.
        # We need to avoid birthday-style collisions for a given cert (such as
        # the pilot) which may issue thousands of proxies per year.
        serial = 0
        while not serial:
            serial = secrets.randbits(64)

        issuerSubject = x509Issuer._certObj.subject

        # The proxy subject is the issuer subject with an extra CN component.
        # And we might as well use the serial.. :)
        subject = x509.Name(
            list(issuerSubject.rdns)
            + [x509.RelativeDistinguishedName([x509.NameAttribute(x509.oid.NameOID.COMMON_NAME, str(serial))])]
        )

        now = datetime.datetime.now(datetime.timezone.utc)
        builder = (
            x509.CertificateBuilder()
            .serial_number(serial)
            .subject_name(subject)
            .issuer_name(issuerSubject)
            .public_key(proxyPublicKey)
            # Set the start of the validity a bit in the past
            # to be sure to be able to use it right now
            .not_valid_before(now - datetime.timedelta(seconds=900))
            .not_valid_after(now + datetime.timedelta(seconds=lifetime))
        )

        for extension, critical in extensions:
            builder = builder.add_extension(extension, critical=critical)

        try:
            proxyCertObj = builder.sign(signingKey, hashes.SHA256())
        except Exception as e:
            return S_ERROR(DErrno.EX509, f"Could not sign the proxy certificate: {e!r}")

        return S_OK(cls(x509Obj=proxyCertObj))

    def load(self, certificate):
        """Load an x509 certificate either from a file or from a string

        :param certificate: path to the file or PEM encoded string

        :returns: S_OK on success, otherwise S_ERROR
        """

        if os.path.exists(certificate):
            return self.loadFromFile(certificate)

        return self.loadFromString(certificate)

    def loadFromFile(self, certLocation):
        """
         Load a x509 cert from a pem file

         :param certLocation: path to the certificate file

        :returns: S_OK / S_ERROR.

        """
        try:
            with open(certLocation) as fd:
                pemData = fd.read()
                return self.loadFromString(pemData)
        except OSError:
            return S_ERROR(DErrno.EOF, f"Can't open {certLocation} file")

    def loadFromString(self, pemData):
        """
        Load a x509 cert from a string containing the pem data

        :param pemData: pem encoded string

        :returns: S_OK / S_ERROR
        """
        if not isinstance(pemData, bytes):
            pemData = pemData.encode("ascii")
        try:
            self._certObj = x509.load_pem_x509_certificate(pemData)
        except Exception as e:
            return S_ERROR(DErrno.ECERTREAD, f"Can't load pem data: {e}")

        self._certLoaded = True
        return S_OK()

    @executeOnlyIfCertLoaded
    def hasExpired(self):
        """
        Check if the loaded certificate is still valid

        :returns: S_OK( True/False )/S_ERROR
        """

        res = self.getNotAfterDate()
        if not res["OK"]:
            return res

        notAfter = res["Value"]
        now = datetime.datetime.utcnow()

        return S_OK(notAfter < now)

    @executeOnlyIfCertLoaded
    def getNotAfterDate(self):
        """
        Get not after date of a certificate

        :returns: S_OK( datetime )/S_ERROR
        """
        # The rest of DIRAC deals with timezone naive datetime objects,
        # so we have to remove the timezone info
        return S_OK(self._certObj.not_valid_after_utc.replace(tzinfo=None))

    @executeOnlyIfCertLoaded
    def getStrength(self):
        """
        Get the length of the key of the certificate in bit

        :returns: S_OK( size )/S_ERROR
        """

        try:
            return S_OK(self._certObj.public_key().key_size)
        except Exception as e:
            return S_ERROR(f"Cannot get certificate strength: {e}")

    @executeOnlyIfCertLoaded
    def getNotBeforeDate(self):
        """
        Get not before date of a certificate

        :returns: S_OK( datetime )/S_ERROR

        """
        # Note: contrary to getNotAfterDate, the timezone info is kept,
        # like it always was
        return S_OK(self._certObj.not_valid_before_utc)

    @executeOnlyIfCertLoaded
    def getSubjectDN(self):
        """
        Get subject DN

        :returns: S_OK( string )/S_ERROR
        """
        return S_OK(asn1_utils.nameToDN(self._certObj.subject))

    @executeOnlyIfCertLoaded
    def getIssuerDN(self):
        """
        Get issuer DN

        :returns: S_OK( string )/S_ERROR
        """
        return S_OK(asn1_utils.nameToDN(self._certObj.issuer))

    @executeOnlyIfCertLoaded
    def getSubjectNameObject(self):
        """
        Get subject name "object". Since the M2Crypto implementation, this
        is only ever used as a string, so that's what it is now.

        :returns: S_OK( str )/S_ERROR
        """
        return S_OK(asn1_utils.nameToDN(self._certObj.subject))

    @executeOnlyIfCertLoaded
    def getPublicKey(self):
        """
        Get the public key of the certificate

        :returns: S_OK(cryptography public key object)

        """
        return S_OK(self._certObj.public_key())

    @executeOnlyIfCertLoaded
    def getSerialNumber(self):
        """
        Get certificate serial number

        :returns: S_OK( serial )/S_ERROR
        """
        return S_OK(self._certObj.serial_number)

    @executeOnlyIfCertLoaded
    def getDIRACGroup(self, ignoreDefault=False):
        """
        Get the dirac group if present

        If no group is found in the certificate, we query the CS to get the default group
        for the given user. This can be disabled using the ignoreDefault parameter

        Note that the lookup in the CS only can work for a proxy of first generation,
        since we search based on the issuer DN

        :param ignoreDefault: if True, do not lookup the CS

        :returns: S_OK(group name/bool)
        """
        try:
            return S_OK(asn1_utils.decodeDIRACGroup(self._certObj))
        except LookupError:
            pass

        if ignoreDefault:
            return S_OK(False)

        # And here is the flaw :)
        result = self.getIssuerDN()
        if not result["OK"]:
            return result
        return Registry.findDefaultGroupForDN(result["Value"])

    @executeOnlyIfCertLoaded
    def hasVOMSExtensions(self):
        """
        Has voms extensions

        :returns: S_OK(bool) if voms extensions are found
        """
        return S_OK(asn1_utils.hasVOMSExtension(self._certObj))

    @executeOnlyIfCertLoaded
    def getVOMSData(self):
        """
        Get voms extensions data

        :returns: S_ERROR/S_OK(dict). For the content of the dict,
              see :py:func:`~DIRAC.Core.Security.asn1_utils.decodeVOMSExtension`
        """
        try:
            vomsExt = asn1_utils.decodeVOMSExtension(self._certObj)
            return S_OK(vomsExt)
        except LookupError:
            return S_ERROR(DErrno.EVOMS, "No VOMS data available")

    @executeOnlyIfCertLoaded
    def generateProxyRequest(self, bitStrength=DEFAULT_PROXY_STRENGTH, limited=False):
        """
        Generate a proxy request. See :py:class:`DIRAC.Core.Security.X509Request.X509Request`

        In principle, there is no reason to have this here, since a the X509Request is independant of
        the X509Certificate when generating it. The only reason is to check whether the current Certificate
        is limited or not.

        :param bitStrength: strength of the key
        :param limited: if True or if the current certificate is limited (see proxy RFC),
                        creates a request for a limited proxy

        :returns: S_OK( :py:class:`DIRAC.Core.Security.X509Request.X509Request` ) / S_ERROR
        """
        if not limited:
            # We check whether "limited proxy" is in the subject
            lastRdn = self._certObj.subject.rdns[-1]
            lastEntry = list(lastRdn)[-1]
            if lastEntry.value == "limited proxy":
                limited = True

        # The import is done here to avoid circular import
        # X509Certificate -> X509Request -> X509Chain -> X509Certificate
        from DIRAC.Core.Security.X509Request import X509Request

        req = X509Request()
        req.generateProxyRequest(bitStrength=bitStrength, limited=limited)

        return S_OK(req)

    @executeOnlyIfCertLoaded
    def getRemainingSecs(self):
        """
        Get remaining lifetime in secs

        :returns: S_OK(remaining seconds)
        """
        notAfter = self.getNotAfterDate()["Value"]
        now = datetime.datetime.utcnow()
        remainingSeconds = max(0, int((notAfter - now).total_seconds()))

        return S_OK(remainingSeconds)

    @executeOnlyIfCertLoaded
    def getExtensions(self):
        """
        Get a decoded list of extensions

        :returns: S_OK( list of tuple (extensionName, extensionValue))
        """
        extList = []
        for extension in self._certObj.extensions:
            name = _EXT_OID_TO_NAME.get(extension.oid.dotted_string, "UNDEF")
            try:
                value = _formatExtensionValue(extension.value)
            except Exception:
                value = "Cannot decode value"
            extList.append((name, value))

        return S_OK(sorted(extList))

    @executeOnlyIfCertLoaded
    def verify(self, pkey):
        """
        Verify the signature of the certificate using the public key provided

        :param pkey: cryptography public key object

        :returns: S_OK(bool) where the boolean shows the success of the verification
        """
        try:
            # The padding (PKCS1v15/PSS for RSA, ECDSA for EC) is derived from
            # the signature algorithm of the certificate itself
            signatureParameters = self._certObj.signature_algorithm_parameters
            if isinstance(pkey, rsa.RSAPublicKey):
                pkey.verify(
                    self._certObj.signature,
                    self._certObj.tbs_certificate_bytes,
                    signatureParameters,
                    self._certObj.signature_hash_algorithm,
                )
            elif isinstance(pkey, ec.EllipticCurvePublicKey):
                pkey.verify(
                    self._certObj.signature,
                    self._certObj.tbs_certificate_bytes,
                    signatureParameters,
                )
            else:
                pkey.verify(self._certObj.signature, self._certObj.tbs_certificate_bytes)
        except Exception:
            return S_OK(False)
        return S_OK(True)

    @executeOnlyIfCertLoaded
    def asPem(self):
        """
        Return certificate as PEM string

        :returns: pem string
        """
        return self._certObj.public_bytes(serialization.Encoding.PEM).decode("ascii")
