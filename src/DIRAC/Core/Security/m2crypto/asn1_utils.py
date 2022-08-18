""" This module contains utilities for parsing extensions in general, but mostly the VOMS extensions.
It has been done based on the reading of the VOMS standard (https://www.ogf.org/documents/GFD.182.pdf)
and on the RFC 5755 (http://www.ietf.org/rfc/rfc5755.txt)

This module relies on definition of the RFC 3281, which is the predecessor of 5755, but it still
seems to work for what we are interested in.

To summarize, the attributes we are interested in are called CertificateAttributes, and are stored in proxy extensions.
The VOMS extension in a proxy is a Sequence of Sequence (??) of CertificateAttribute. One Sequence is due to the fact
that you can embed more than one VO CertificateAttribute in one proxy. The other one was acknowledge as a an error in
the formal description (an Errata will come)

This is now pure python, but it might be interesting to wrap the existing
C library (https://github.com/italiangrid/voms) instead...

"""
from pyasn1.codec.der.decoder import decode as der_decode
from pyasn1.error import PyAsn1Error
from pyasn1.type import namedtype, univ, char as asn1char
from pyasn1_modules import rfc2459, rfc3281
from DIRAC.Core.Security.m2crypto import (
    VOMS_EXTENSION_OID,
    VOMS_FQANS_OID,
    DN_MAPPING,
    VOMS_TAGS_EXT_OID,
    DIRAC_GROUP_OID,
)


class _ACSequence(univ.SequenceOf):
    """This describe a sequence of AC, as per GFD 182"""

    componentType = rfc3281.AttributeCertificate()


class _ACSequenceOfSequence(univ.SequenceOf):
    """A Sequence of Sequence of AC. In contradiction to GFD formal description,
    but that's what it is
    """

    componentType = _ACSequence()


# The Tag structure is used to describe things like the nickname
# (See OGF 3.6.4)
# Similarely to above, there is an extra Sequence layer above the TagContainer
class _VOMSTag(univ.Sequence):
    """Tag as per GOF 182"""

    componentType = namedtype.NamedTypes(
        namedtype.NamedType("name", univ.OctetString()),
        namedtype.NamedType("value", univ.OctetString()),
        namedtype.NamedType("qualifier", univ.OctetString()),
    )


class _VOMSTags(univ.SequenceOf):
    """Sequence of VOMSTag as per GOF 182"""

    componentType = _VOMSTag()


class _TagList(univ.Sequence):
    """TagList as per GOF 182"""

    componentType = namedtype.NamedTypes(
        namedtype.NamedType("policyAuthority", rfc2459.GeneralNames()), namedtype.NamedType("tags", _VOMSTags())
    )


class _TagContainer(univ.SequenceOf):
    """TagContainer as per GOF 182"""

    componentType = _TagList()


class _TagContainers(univ.SequenceOf):
    """Sequence of TagContainer
    Note sure why it is here, but that's how it is encoded
    """

    componentType = _TagContainer()


def decodeDIRACGroup(m2cert):
    """Decode the content of the dirac group extension

    :param m2cert: M2crypto x509 object, a certificate

    :returns: the dirac group

    :raises: same as retrieveExtension
    """

    diracGroupOctetString = retrieveExtension(m2cert, DIRAC_GROUP_OID)
    diracGroupUTF8Str, _rest = der_decode(diracGroupOctetString, asn1Spec=asn1char.IA5String())

    return diracGroupUTF8Str.asOctets().decode()


def _decodeASN1String(rdnNameAttrValue):
    """Tries to decode a string encoded with the following type:
    * BMPString
    * IA5String
    * PrintableString
    * TeletexString
    * UTF8String

    Most of these types come from the definition of the issuer field in RFC3280:
    * The basic attributes, defined as DirectoryString (4.1.2.4  Issuer)
    * the optional attributes (Appendix A.  Psuedo-ASN.1 Structures and OIDs)

    This utility function is needed for 2 reasons:
    * Not all the attributes are encoded the same way, and as we do not want to bother
      with zillions of `if` conditions, we may just as well try
    * It seems like the RFCs are not always respected, and the encoding is not always the correct one

    http://www.oid-info.com/ is a very good source of information for looking up the type of
    a specific OID

    :param rdnNameAttrValue: the value part of rfc3280.AttributeTypeAndValue

    :returns: the decoded value or raises PyAsn1Error if nothing worked
    """
    for decodeType in (
        asn1char.UTF8String,
        asn1char.PrintableString,
        asn1char.IA5String,
        asn1char.TeletexString,
        asn1char.BMPString,
    ):
        try:
            attrValStr, _rest = der_decode(rdnNameAttrValue, decodeType())
        # Decoding error, try the next type
        except PyAsn1Error:
            pass
        else:
            # If the decoding worked, return it
            return attrValStr
    raise PyAsn1Error("Could not find a correct decoding type")


def hasVOMSExtension(m2cert):
    """Utility fonction to check if the certificate has VOMS extensions

    :param m2cert: M2Crypto X509 object, a certificate

    :returns: boolean
    """
    try:
        retrieveExtension(m2cert, VOMS_EXTENSION_OID)
        return True
    except LookupError:
        return False


def decodeVOMSExtension(m2cert):
    """Decode the content of the VOMS extension

    :param m2cert: M2Crypto X509 object, a certificate

    :returns: A dictionary containing the following fields:

      * notBefore: datetime.datetime
      * notAfter: datetime.datetime
      * attribute: (string). Comma separated list of VOMS tags presented as below

                             "<tagName> = <tagValue> (<tagQualifier>)"
                             Typically, the nickname will look like
                             'nickname = chaen (lhcb)',

      * fqan: List of VOMS "position" (['/lhcb/Role=production/Capability=NULL', '/lhcb/Role=NULL/Capability=NULL'])
      * vo: name of the VO,
      * subject: subject DN to which the attributes were granted,
      * issuer: typically the DN of the VOMS server (e.g '/DC=ch/DC=cern/OU=computers/CN=lcg-voms2.cern.ch')

    """
    vomsExtensionDict = {}
    vomsExtensionOctetString = retrieveExtension(m2cert, VOMS_EXTENSION_OID)
    # Decode it as a ACSequenceOfSequence, which is what it is...
    vomsExtensionSeqOfSeq, _rest = der_decode(vomsExtensionOctetString, asn1Spec=_ACSequenceOfSequence())

    # In principle, according to GFD 182, there could be more than one VO VOMS AC per proxy.
    # The standard specifies that we have to accept at least the first one, which is what
    # I will do...
    vomsCertAttribute = vomsExtensionSeqOfSeq[0][0]

    ######
    # TODO in principle, we should check the signature of the Attribute...
    # _signatureAlgorith = vomsCertAttribute['signatureAlgorithm']
    # _signatureValue = vomsCertAttribute['signatureValue']
    ######

    certAttrInfo = vomsCertAttribute["acinfo"]

    # pyasn1 does things correctly by setting a timezone info in the datetime
    # However, we do not in DIRAC, and so we can't compare the dates.
    # We have to remove the timezone info from the datetime objects

    notBefore = certAttrInfo["attrCertValidityPeriod"]["notBeforeTime"].asDateTime
    vomsExtensionDict["notBefore"] = notBefore.replace(tzinfo=None)

    notAfter = certAttrInfo["attrCertValidityPeriod"]["notAfterTime"].asDateTime
    vomsExtensionDict["notAfter"] = notAfter.replace(tzinfo=None)

    # ######### Retrieving the issuer ##########
    # Get the issuer. A bit tricky, because we have to reconstruct the full DN ourselves
    # The GFD 182 and RFC 3281 give enough restriction such that we can afford some direct
    # [0] access

    issuer = ""

    # rdnName is a rfc3280.RelativeDistinguishedName object
    for rdnName in certAttrInfo["issuer"]["v2Form"]["issuerName"][0]["directoryName"]["rdnSequence"]:
        # rdnNameAttr rfc3280.AttributeTypeAndValue'
        rdnNameAttr = rdnName[0]

        attrOid = ".".join([str(e) for e in rdnNameAttr["type"].asTuple()])

        # Now finally convert the last part into a asn1char.*String
        attrValStr = _decodeASN1String(rdnNameAttr["value"])
        attrVal = attrValStr.asOctets().decode()
        #
        issuer += f"{DN_MAPPING[attrOid]}{attrVal}"

    vomsExtensionDict["issuer"] = issuer

    # ### Issuer retrieved #####

    # ## Retrieving the Subject ####
    # We have to do the same for the subject than for the issuer

    subject = ""

    # rdnName is a rfc3280.RelativeDistinguishedName object
    for rdnName in certAttrInfo["holder"]["baseCertificateID"]["issuer"][0]["directoryName"]["rdnSequence"]:
        # rdnNameAttr rfc3280.AttributeTypeAndValue'
        rdnNameAttr = rdnName[0]

        attrOid = ".".join([str(e) for e in rdnNameAttr["type"].asTuple()])
        # # Because there are non printable characters in the values (new line, etc)
        # # we have to get ride of them. The best way is to get them as number, and make sure it is a
        # # a printable char (between 32 and 126)
        #
        # attrVal = ''.join([chr(c) for c in rdnNameAttr['value'].asNumbers() if 32 <= c <= 126 ])

        # Now finally convert the last part into a asn1char.*String
        attrValStr = _decodeASN1String(rdnNameAttr["value"])
        attrVal = attrValStr.asOctets().decode()

        subject += f"{DN_MAPPING[attrOid]}{attrVal}"

    vomsExtensionDict["subject"] = subject

    # ### Retrieving the FQAN ####

    # According to GFD182, there may be more attributes that just the FQAN, even though it
    # does not seem to be the case in practice. So we make sure to have the good one
    fqanOIDObj = univ.ObjectIdentifier(VOMS_FQANS_OID)

    # There shall be only one, hense the [0]
    # This is an rfc3280.Attribute object
    fqanAttrObj = [attrObj for attrObj in certAttrInfo["attributes"] if attrObj["type"] == fqanOIDObj][0]

    # According to GFD182 3.4.1, we decode the value as a IetfAttrSyntax.
    # Since multiple values are not allowed, just take the first item
    #
    fqanObj, _rest = der_decode(fqanAttrObj["values"][0], asn1Spec=rfc3281.IetfAttrSyntax())

    # We retrieve the VO and the VOMS server
    voName, _, _ = fqanObj["policyAuthority"][0]["uniformResourceIdentifier"].asOctets().decode().split(":")

    vomsExtensionDict["vo"] = voName

    # Now retrieve the position of the holder (group, role)
    fqanList = []
    for fqanPositionObj in fqanObj["values"]:
        fqanList.append(fqanPositionObj["octets"].asOctets().decode())

    vomsExtensionDict["fqan"] = fqanList

    # ############ End of the FQAN ################

    # Now the Tags, called attributes in the dict...

    tagDescriptions = []
    vomsTagsOIDObj = univ.ObjectIdentifier(VOMS_TAGS_EXT_OID)

    # First find the tag containers
    tagExtensionObj = [extObj for extObj in certAttrInfo["extensions"] if extObj["extnID"] == vomsTagsOIDObj]

    # If we found tags
    if tagExtensionObj:
        # Multiple is forbiden, so only one tag container
        tagExtensionObj = tagExtensionObj[0]

        tagContainersObj, _rest = der_decode(tagExtensionObj["extnValue"], asn1Spec=_TagContainers())

        # TODO in principle, we should check that this value
        # and the one of the policyAuthority of the fqan are the same
        # _tagPolicyAuthority = tagContainersObj[0][0]['policyAuthority'][0]['uniformResourceIdentifier'] \
        #     .asOctets().decode()
        ######

        for tagContainer in tagContainersObj:
            for tagList in tagContainer:
                # Note: it is here that I should check the policyAuthority
                tagList = tagList["tags"]
                for tag in tagList:
                    # This gives a string like
                    # nickname = chaen (lhcb)
                    tagDescriptions.append(
                        "%s = %s (%s)"
                        % (
                            tag["name"].asOctets().decode(),
                            tag["value"].asOctets().decode(),
                            tag["qualifier"].asOctets().decode(),
                        )
                    )

        vomsExtensionDict["attribute"] = ",".join(tagDescriptions)

    # #### Tags are done ################

    return vomsExtensionDict


def retrieveExtension(m2Cert, extensionOID):
    """Retrieves the extension from a certificate from its OID

    :param m2Cert: M2Crypto X509 object, a certificate
    :param extensionOID: the OID we are looking for

    :returns: an ~pyasn1.type.univ.OctetString object, which is the content of the extension
              (it still needs to be deserialized, depending on the extension !)

    :raises: LookupError if it does not have the extension
    """

    # Decode the certificate as a RFC2459 Certificate object.It is compatible
    # with the RFC proxy definition
    cert, _rest = der_decode(m2Cert.as_der(), asn1Spec=rfc2459.Certificate())
    extensions = cert["tbsCertificate"]["extensions"]

    # Construct an OID object for comparison purpose
    extensionOIDObj = univ.ObjectIdentifier(extensionOID)

    # We check every extension OID. This will be necessary until M2Crypto
    # allows to register OID alias (https://gitlab.com/m2crypto/m2crypto/issues/231)
    for extension in extensions:
        # We found the good extension
        if extension["extnID"] == extensionOIDObj:
            return extension["extnValue"]

    # If we are here, it means that we could not find the expected extension.
    raise LookupError("Could not find extension with OID %s" % extensionOID)
