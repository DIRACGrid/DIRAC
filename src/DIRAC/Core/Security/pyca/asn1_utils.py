""" This module contains utilities for parsing X509 extensions, mostly the VOMS extensions.
It has been done based on the reading of the VOMS standard (https://www.ogf.org/documents/GFD.182.pdf)
and on the RFC 5755 (http://www.ietf.org/rfc/rfc5755.txt)

This module relies on definition of the RFC 3281, which is the predecessor of 5755, but it still
seems to work for what we are interested in.

To summarize, the attributes we are interested in are called CertificateAttributes, and are stored in proxy extensions.
The VOMS extension in a proxy is a Sequence of Sequence (??) of CertificateAttribute. One Sequence is due to the fact
that you can embed more than one VO CertificateAttribute in one proxy. The other one was acknowledge as a an error in
the formal description (an Errata will come)

The ASN.1 structures are expressed with the declarative API of pyca/cryptography
(cryptography.hazmat.asn1), which does the parsing in Rust. The RFC 3281 module is
DEFINITIONS IMPLICIT TAGS; only the fields actually emitted by VOMS are modelled.
"""
from typing import Annotated

from cryptography import x509
from cryptography.hazmat import asn1

from DIRAC.Core.Security import (
    DIRAC_GROUP_OID,
    DN_MAPPING,
    PROXY_CERT_INFO_EXTENSION_OID,
    VOMS_EXTENSION_OID,
    VOMS_FQANS_OID,
    VOMS_TAGS_EXT_OID,
)

# GeneralName is a 9-way CHOICE in the RFC, but x509.Name is not supported
# inside asn1.Variant, so we model the two alternatives VOMS actually emits
# (directoryName [4], uniformResourceIdentifier [6]) as concrete per-context
# types instead.


@asn1.sequence
class AttributeTypeAndValue:
    type: x509.ObjectIdentifier
    value: asn1.TLV  # ANY: a DirectoryString variant, decoded by tag in _decodeASN1String


#: RelativeDistinguishedName ::= SET OF AttributeTypeAndValue
Rdn = asn1.SetOf[AttributeTypeAndValue]
#: GeneralName/directoryName: [4] EXPLICIT Name (explicit because Name is a CHOICE)
DirName = Annotated[list[Rdn], asn1.Explicit(4)]
#: GeneralName/uniformResourceIdentifier: [6] IMPLICIT IA5String
UriName = Annotated[asn1.IA5String, asn1.Implicit(6)]


@asn1.sequence
class IssuerSerial:
    issuer: list[DirName]
    serial: int
    issuerUID: asn1.BitString | None


@asn1.sequence
class Holder:
    baseCertificateID: Annotated[IssuerSerial | None, asn1.Implicit(0)]
    entityName: Annotated[list[DirName] | None, asn1.Implicit(1)]
    # objectDigestInfo [2] OPTIONAL omitted: never present in VOMS ACs


@asn1.sequence
class V2Form:
    issuerName: list[DirName] | None
    baseCertificateID: Annotated[IssuerSerial | None, asn1.Implicit(0)]


@asn1.sequence
class AttCertValidityPeriod:
    notBeforeTime: asn1.GeneralizedTime
    notAfterTime: asn1.GeneralizedTime


@asn1.sequence
class Attribute:
    type: x509.ObjectIdentifier
    values: asn1.SetOf[asn1.TLV]


@asn1.sequence
class Extension:
    extnID: x509.ObjectIdentifier
    critical: Annotated[bool, asn1.Default(False)]
    extnValue: bytes


@asn1.sequence
class AttributeCertificateInfo:
    version: int
    holder: Holder
    # AttCertIssuer is a CHOICE, but v2 attribute certificates always use v2Form
    issuer: Annotated[V2Form, asn1.Implicit(0)]
    # AlgorithmIdentifier contains an `ANY OPTIONAL`, and we never read it,
    # so keep it as an opaque TLV
    signature: asn1.TLV
    serialNumber: int
    attrCertValidityPeriod: AttCertValidityPeriod
    attributes: list[Attribute]
    issuerUniqueID: asn1.BitString | None
    extensions: list[Extension] | None


@asn1.sequence
class AttributeCertificate:
    acinfo: AttributeCertificateInfo
    signatureAlgorithm: asn1.TLV
    signatureValue: asn1.BitString


@asn1.sequence
class IetfAttrSyntax:
    policyAuthority: Annotated[list[UriName] | None, asn1.Implicit(0)]
    values: list[bytes]


# The Tag structure is used to describe things like the nickname
# (See OGF 3.6.4)
@asn1.sequence
class _VOMSTag:
    name: bytes
    value: bytes
    qualifier: bytes


@asn1.sequence
class _TagList:
    policyAuthority: list[UriName]
    tags: list[_VOMSTag]


# asn1.decode_der only accepts a registered class as the root, so the outer
# SEQUENCE OF layers get one-field wrapper classes. This is byte-identical to
# a single-element SEQUENCE OF, which is what is emitted in practice (and all
# that was ever supported: the previous implementation only read [0][0]).
@asn1.sequence
class _ACSequenceOfSequence:
    acs: list[AttributeCertificate]


@asn1.sequence
class _TagContainers:
    tagLists: list[_TagList]


# RFC 3820 proxyCertInfo, used both for generating and validating proxies
@asn1.sequence
class ProxyPolicy:
    policyLanguage: x509.ObjectIdentifier
    policy: bytes | None


@asn1.sequence
class ProxyCertInfo:
    pCPathLenConstraint: int | None
    proxyPolicy: ProxyPolicy


def retrieveExtension(cert, extensionOID):
    """Retrieves the raw content of a certificate extension from its OID

    :param cert: cryptography.x509.Certificate object
    :param extensionOID: the OID we are looking for, as a dotted string

    :returns: bytes, the DER content of the extension
              (it still needs to be deserialized, depending on the extension !)

    :raises: LookupError if it does not have the extension
    """
    try:
        ext = cert.extensions.get_extension_for_oid(x509.ObjectIdentifier(extensionOID))
    except x509.ExtensionNotFound as e:
        raise LookupError(f"Could not find extension with OID {extensionOID}") from e
    value = ext.value
    return value.public_bytes() if isinstance(value, x509.UnrecognizedExtension) else value.value


def decodeDIRACGroup(cert):
    """Decode the content of the dirac group extension

    :param cert: cryptography.x509.Certificate object

    :returns: the dirac group

    :raises: same as retrieveExtension
    """
    diracGroupData = retrieveExtension(cert, DIRAC_GROUP_OID)
    return asn1.decode_der(asn1.IA5String, diracGroupData).as_str()


def encodeDIRACGroup(diracGroup):
    """Encode a dirac group as the content of the dirac group extension

    :param diracGroup: the group name

    :returns: bytes, DER encoded IA5String
    """
    return asn1.encode_der(asn1.IA5String(diracGroup))


def encodeProxyCertInfo(policyOID):
    """Encode an RFC 3820 proxyCertInfo extension content

    :param policyOID: dotted string of the proxy policy language OID

    :returns: bytes, DER encoded ProxyCertInfo
    """
    proxyCertInfo = ProxyCertInfo(
        pCPathLenConstraint=None,
        proxyPolicy=ProxyPolicy(policyLanguage=x509.ObjectIdentifier(policyOID), policy=None),
    )
    return asn1.encode_der(proxyCertInfo)


def decodeProxyCertInfo(cert):
    """Retrieve and decode the RFC 3820 proxyCertInfo extension of a certificate

    :param cert: cryptography.x509.Certificate object

    :returns: ProxyCertInfo object

    :raises: LookupError if the certificate has no proxyCertInfo extension
    """
    proxyCertInfoData = retrieveExtension(cert, PROXY_CERT_INFO_EXTENSION_OID)
    return asn1.decode_der(ProxyCertInfo, proxyCertInfoData)


def _decodeASN1String(tlv):
    """Decode an ASN.1 DirectoryString-ish value kept as a raw TLV.

    Historically (RFC 3280 & reality) the following types are found:
    BMPString, IA5String, PrintableString, TeletexString, UTF8String.
    BMPString is UTF-16-BE; everything else decodes fine as UTF-8.

    :param tlv: asn1.TLV object, the value part of AttributeTypeAndValue

    :returns: the decoded string
    """
    codec = "utf-16-be" if tlv.tag_bytes[0] == 0x1E else "utf-8"
    return bytes(tlv.data).decode(codec)


def _dirNameToDN(rdnSequence):
    """Reconstruct a DN string like '/O=Dirac Computing/O=CERN/CN=MrUser' from
    a decoded directoryName (list of Rdn)

    :param rdnSequence: list of Rdn objects

    :returns: the DN as a string
    """
    dn = ""
    for rdn in rdnSequence:
        # The GFD 182 and RFC 3281 give enough restriction such that we can afford
        # taking the first attribute only, like the previous implementation did
        attr = rdn.as_list()[0]
        dn += f"{DN_MAPPING[attr.type.dotted_string]}{_decodeASN1String(attr.value)}"
    return dn


def hasVOMSExtension(cert):
    """Utility function to check if the certificate has VOMS extensions

    :param cert: cryptography.x509.Certificate object

    :returns: boolean
    """
    try:
        retrieveExtension(cert, VOMS_EXTENSION_OID)
        return True
    except LookupError:
        return False


def decodeVOMSExtension(cert):
    """Decode the content of the VOMS extension

    :param cert: cryptography.x509.Certificate object

    :returns: A dictionary containing the following fields:

      * notBefore: datetime.datetime
      * notAfter: datetime.datetime
      * attribute: (string). Comma separated list of VOMS tags presented as bellow

                             "<tagName> = <tagValue> (<tagQualifier>)"
                             Typically, the nickname will look like
                             'nickname = chaen (lhcb)',

      * fqan: List of VOMS "position" (['/lhcb/Role=production/Capability=NULL', '/lhcb/Role=NULL/Capability=NULL'])
      * vo: name of the VO,
      * subject: subject DN to which the attributes were granted,
      * issuer: typically the DN of the VOMS server (e.g '/DC=ch/DC=cern/OU=computers/CN=lcg-voms2.cern.ch')

    :raises: LookupError if the certificate has no VOMS extension
    """
    vomsExtensionDict = {}
    vomsExtensionData = retrieveExtension(cert, VOMS_EXTENSION_OID)

    # In principle, according to GFD 182, there could be more than one VO VOMS AC per proxy.
    # The standard specifies that we have to accept at least the first one, which is what
    # we do...
    vomsCertAttribute = asn1.decode_der(_ACSequenceOfSequence, vomsExtensionData).acs[0]

    ######
    # TODO in principle, we should check the signature of the Attribute...
    # (vomsCertAttribute.signatureAlgorithm / signatureValue)
    ######

    certAttrInfo = vomsCertAttribute.acinfo

    # The declarative API does things correctly by setting a timezone info in the datetime
    # However, we do not in DIRAC, and so we can't compare the dates.
    # We have to remove the timezone info from the datetime objects
    validity = certAttrInfo.attrCertValidityPeriod
    vomsExtensionDict["notBefore"] = validity.notBeforeTime.as_datetime().replace(tzinfo=None)
    vomsExtensionDict["notAfter"] = validity.notAfterTime.as_datetime().replace(tzinfo=None)

    # The issuer and holder DNs have to be reconstructed from the rdnSequence
    # of their directoryName
    vomsExtensionDict["issuer"] = _dirNameToDN(certAttrInfo.issuer.issuerName[0])
    vomsExtensionDict["subject"] = _dirNameToDN(certAttrInfo.holder.baseCertificateID.issuer[0])

    # ### Retrieving the FQAN ####

    # According to GFD182, there may be more attributes than just the FQAN, even though it
    # does not seem to be the case in practice. So we make sure to have the good one
    fqanOIDObj = x509.ObjectIdentifier(VOMS_FQANS_OID)

    # There shall be only one, hence the [0]
    fqanAttrObj = [attrObj for attrObj in certAttrInfo.attributes if attrObj.type == fqanOIDObj][0]

    # According to GFD182 3.4.1, we decode the value as a IetfAttrSyntax.
    # Since multiple values are not allowed, just take the first item
    fqanObj = fqanAttrObj.values.as_list()[0].parse(IetfAttrSyntax)

    # We retrieve the VO and the VOMS server
    voName, _, _ = fqanObj.policyAuthority[0].as_str().split(":")
    vomsExtensionDict["vo"] = voName

    # Now retrieve the position of the holder (group, role)
    vomsExtensionDict["fqan"] = [fqanPosition.decode() for fqanPosition in fqanObj.values]

    # ############ End of the FQAN ################

    # Now the Tags, called attributes in the dict...

    tagDescriptions = []
    vomsTagsOIDObj = x509.ObjectIdentifier(VOMS_TAGS_EXT_OID)

    # First find the tag containers
    tagExtensionObj = [extObj for extObj in (certAttrInfo.extensions or []) if extObj.extnID == vomsTagsOIDObj]

    # If we found tags
    if tagExtensionObj:
        # Multiple is forbidden, so only one tag container
        tagContainers = asn1.decode_der(_TagContainers, tagExtensionObj[0].extnValue)

        # TODO in principle, we should check that the policyAuthority
        # and the one of the fqan are the same
        for tagList in tagContainers.tagLists:
            for tag in tagList.tags:
                # This gives a string like
                # nickname = chaen (lhcb)
                tagDescriptions.append(f"{tag.name.decode()} = {tag.value.decode()} ({tag.qualifier.decode()})")

        vomsExtensionDict["attribute"] = ",".join(tagDescriptions)

    # #### Tags are done ################

    return vomsExtensionDict


# Mapping from OID to the OpenSSL short names, used to render DNs the same way
# X509_NAME_oneline (i.e. M2Crypto's str(X509_Name)) used to
_OID_TO_SHORT_NAME = {
    "2.5.4.3": "CN",
    "2.5.4.4": "SN",
    "2.5.4.5": "serialNumber",
    "2.5.4.6": "C",
    "2.5.4.7": "L",
    "2.5.4.8": "ST",
    "2.5.4.9": "street",
    "2.5.4.10": "O",
    "2.5.4.11": "OU",
    "2.5.4.12": "title",
    "2.5.4.42": "GN",
    "2.5.4.43": "initials",
    "2.5.4.46": "dnQualifier",
    "2.5.4.65": "pseudonym",
    "0.9.2342.19200300.100.1.1": "UID",
    "0.9.2342.19200300.100.1.25": "DC",
    "1.2.840.113549.1.9.1": "emailAddress",
}


def _onelineEscape(value) -> str:
    """Escape an attribute value the way X509_NAME_oneline does:
    bytes outside the printable ASCII range are rendered as ``\\xHH``.
    This keeps DNs of certificates with non-ASCII characters identical
    to what they always were (e.g. in the Registry).
    """
    raw = value.encode("utf-8") if isinstance(value, str) else bytes(value)
    if all(0x20 <= char <= 0x7E for char in raw):
        return raw.decode("ascii")
    return "".join(chr(char) if 0x20 <= char <= 0x7E else f"\\x{char:02X}" for char in raw)


def nameToDN(name: x509.Name) -> str:
    """Render a cryptography x509.Name in the OpenSSL oneline format,
    e.g. '/O=Dirac Computing/O=CERN/CN=MrUser'

    :param name: cryptography.x509.Name object

    :returns: the DN as a string
    """
    dn = ""
    for rdn in name.rdns:
        for attr in rdn:
            shortName = _OID_TO_SHORT_NAME.get(attr.oid.dotted_string, attr.oid.dotted_string)
            dn += f"/{shortName}={_onelineEscape(attr.value)}"
    return dn
