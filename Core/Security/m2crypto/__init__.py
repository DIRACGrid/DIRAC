# List of OIDs used in handling VOMS extension.
# VOMS extension is encoded in ASN.1 format and it's surprisingly hard to decode. OIDs describe content of sections
# of the data. There is no "official list of OIDs", ones used here are sourced from analyzing VOMS extensions itself
# and different pieces of code and presentations in subject of X509 certificates, certificate extensions and VOMS.
# Googling names or values of those OIDs, especially VOMS related, usually result in up to three pages of results,
# mainly Java code defining those values like code below.
# This is literally lookup table, so I know WTH is this, when I read value and see '1.3.6.1.4.1.8005.100.100.4'.
VOMS_EXTENSION_OID = '1.3.6.1.4.1.8005.100.100.5'
VOMS_FQANS_OID = '1.3.6.1.4.1.8005.100.100.4'
VOMS_TAGS_EXT_OID = '1.3.6.1.4.1.8005.100.100.11'
DOMAIN_COMPONENT_OID = '0.9.2342.19200300.100.1.25'
ORGANIZATIONAL_UNIT_NAME_OID = '2.5.4.11'
COMMON_NAME_OID = '2.5.4.3'
ORGANIZATION_NAME = '2.5.4.10'
DIRAC_GROUP_OID = '1.2.42.42'


# See https://tools.ietf.org/html/rfc3820#appendix-A
PROXY_OID = '1.3.6.1.5.5.7.21.1'
LIMITED_PROXY_OID = '1.3.6.1.4.1.3536.1.1.1.9'

DN_MAPPING = {
    DOMAIN_COMPONENT_OID: '/DC=',
    ORGANIZATIONAL_UNIT_NAME_OID: '/OU=',
    COMMON_NAME_OID: '/CN=',
    ORGANIZATION_NAME: '/O=',
}
