""" ProxyProvider implementation for the proxy generation using local (DIRAC) CA credentials

    This class is a simple, limited CA, its main purpose is to generate a simple proxy for DIRAC users
    who do not have any certificate register on the fly.

    Required parameters in the DIRAC configuration for its implementation:

    .. literalinclude:: /dirac.cfg
      :start-after: ## DIRACCA type:
      :end-before: ##
      :dedent: 2
      :caption: /Resources/ProxyProviders section

    Also, as an additional feature, this class can read properties from a simple openssl CA configuration file.
    To do this, just set the path to an existing configuration file as a CAConfigFile parameter. In this case,
    the distinguished names order in the created proxy will be the same as in the configuration file policy block.

    The Proxy provider supports the following distinguished names
    (https://www.cryptosys.net/pki/manpki/pki_distnames.html)::

      SN(surname)
      GN(givenName)
      C(countryName)
      CN(commonName)
      L(localityName)
      Email(emailAddress)
      O(organizationName)
      OU(organizationUnitName)
      SP,ST(stateOrProvinceName)
      SERIALNUMBER(serialNumber)

"""
import re
import secrets
import datetime
import collections

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Security import asn1_utils
from DIRAC.Core.Security.X509Chain import X509Chain  # pylint: disable=import-error
from DIRAC.Resources.ProxyProvider.ProxyProvider import ProxyProvider

# Mapping between the distinguished name aliases and their OID, replacing
# the M2Crypto X509_Name.nid lookup table
_FIELDS_TO_OID = {
    "C": "2.5.4.6",
    "countryName": "2.5.4.6",
    "SP": "2.5.4.8",
    "ST": "2.5.4.8",
    "stateOrProvinceName": "2.5.4.8",
    "L": "2.5.4.7",
    "localityName": "2.5.4.7",
    "O": "2.5.4.10",
    "organizationName": "2.5.4.10",
    "OU": "2.5.4.11",
    "organizationalUnitName": "2.5.4.11",
    "CN": "2.5.4.3",
    "commonName": "2.5.4.3",
    "Email": "1.2.840.113549.1.9.1",
    "emailAddress": "1.2.840.113549.1.9.1",
    "SN": "2.5.4.4",
    "surname": "2.5.4.4",
    "GN": "2.5.4.42",
    "givenName": "2.5.4.42",
    "serialNumber": "2.5.4.5",
    "SERIALNUMBER": "2.5.4.5",
    "DC": "0.9.2342.19200300.100.1.25",
    "domainComponent": "0.9.2342.19200300.100.1.25",
}

_ALGORITHMS = {
    "sha1": hashes.SHA1,
    "sha224": hashes.SHA224,
    "sha256": hashes.SHA256,
    "sha384": hashes.SHA384,
    "sha512": hashes.SHA512,
}


class DIRACCAProxyProvider(ProxyProvider):
    def __init__(self, parameters=None):
        """Constructor"""
        super().__init__(parameters)
        self.log = gLogger.getSubLogger(__name__)
        # Initialize
        self.maxDict = {}
        self.minDict = {}
        self.bits = 2048
        self.algoritm = "sha256"
        self.match = []
        self.supplied = [_FIELDS_TO_OID["CN"]]
        self.optional = [_FIELDS_TO_OID[f] for f in ("C", "O", "OU", "emailAddress")]
        self.dnList = ["C", "O", "OU", "CN", "emailAddress"]
        # Distinguished names
        self.fields2oid = dict(_FIELDS_TO_OID)
        self.oid2fields = {}  # oid: list of distinguished names
        # Specify standart fields
        for field, oid in self.fields2oid.items():
            self.oid2fields.setdefault(oid, []).append(field)
        self.dnInfoDictCA = {}
        # List of x509.NameAttribute of the DN being built
        self.__nameAttributes = []

    def setParameters(self, parameters):
        """Set new parameters

        :param dict parameters: provider parameters

        :return: S_OK()/S_ERROR()
        """
        for k, v in parameters.items():
            if not isinstance(v, list) and k in ["Match", "Supplied", "Optional", "DNOrder"] + list(self.fields2oid):
                parameters[k] = v.replace(", ", ",").split(",")
        self.parameters = parameters
        # If CA configuration file exist
        if parameters.get("CAConfigFile"):
            self.__parseCACFG()
        if "Bits" in parameters:
            self.bits = int(parameters["Bits"])
        if "Algoritm" in parameters:
            self.algoritm = parameters["Algoritm"]
        if "Match" in parameters:
            self.match = [self.fields2oid[f] for f in parameters["Match"]]
        if "Supplied" in parameters:
            self.supplied = [self.fields2oid[f] for f in parameters["Supplied"]]
        if "Optional" in parameters:
            self.optional = [self.fields2oid[f] for f in parameters["Optional"]]
        allFields = self.optional + self.supplied + self.match
        if "DNOrder" in parameters:
            self.dnList = []
            if not any([any([f in parameters["DNOrder"] for f in self.oid2fields[n]]) for n in allFields]):
                return S_ERROR("DNOrder must contain all configured fields.")
            for field in parameters["DNOrder"]:
                if self.fields2oid[field] in allFields:
                    self.dnList.append(field)

        # Set defaults for distridutes names
        self.oid2defField = {}
        for field, value in list(self.parameters.items()):
            if field in self.fields2oid and self.fields2oid[field] in allFields:
                self.parameters[self.fields2oid[field]] = value
                self.oid2defField[self.fields2oid[field]] = field

        # Read CA certificate
        chain = X509Chain()
        result = chain.loadChainFromFile(self.parameters["CertFile"])
        if result["OK"]:
            result = chain.getCredentials()
            if result["OK"]:
                result = self.__parseDN(result["Value"]["subject"])
        if not result["OK"]:
            return result
        self.dnInfoDictCA = result["Value"]
        return S_OK()

    def checkStatus(self, userDN):
        """Read ready to work status of proxy provider

        :param str userDN: user DN

        :return: S_OK()/S_ERROR()
        """
        self.log.debug("Ckecking work status of", self.parameters["ProviderName"])
        result = self.__parseDN(userDN)
        if not result["OK"]:
            return result
        dnInfoDict = result["Value"]

        try:
            userOIDs = [self.fields2oid[f.split("=")[0]] for f in userDN.lstrip("/").split("/")]
        except (ValueError, KeyError) as e:
            return S_ERROR(f"Unknown DN field in used DN: {e}")
        oidOrder = [self.fields2oid[f] for f in self.dnList]
        for index, oid in enumerate(userOIDs):
            if oid not in oidOrder:
                return S_ERROR(
                    f'"{self.oid2defField.get(oid, min(self.oid2fields[oid], key=len))}" field not found in order.'
                )
            if index > oidOrder.index(oid):
                return S_ERROR("Bad DNs order")
            for i in range(oidOrder.index(oid) - 1):
                try:
                    if userOIDs.index(oidOrder[i]) > index:
                        return S_ERROR("Bad DNs order")
                except (ValueError, KeyError):
                    continue
            for i in range(oidOrder.index(oid) + 1, len(oidOrder)):
                try:
                    if userOIDs.index(oidOrder[i]) < index:
                        return S_ERROR("Bad DNs order")
                except (ValueError, KeyError):
                    continue

        for oid in self.supplied:
            if oid not in [self.fields2oid[f] for f in dnInfoDict]:
                return S_ERROR(
                    'Current DN is invalid, "%s" field must be set.'
                    % self.oid2defField.get(oid, min(self.oid2fields[oid], key=len))
                )

        for field, values in dnInfoDict.items():
            oid = self.fields2oid[field]
            err = f'Current DN is invalid, "{field}" field'
            if oid not in self.supplied + self.match + self.optional:
                return S_ERROR(f"{err} is not found for current CA.")
            if oid in self.match and not self.dnInfoDictCA[field] == values:
                return S_ERROR(f"{err} must be /{field}={('/%s=' % field).joing(self.dnInfoDictCA[field])}.")
            if oid in self.maxDict:
                rangeMax = list(range(min(len(values), len(self.maxDict[oid]))))
                if any([True if len(values[i]) > self.maxDict[oid][i] else False for i in rangeMax]):
                    return S_ERROR(f"{err} values must be less then {', '.join(self.maxDict[oid])}.")
            if oid in self.minDict:
                rangeMin = list(range(min(len(values), len(self.minDict[oid]))))
                if any([True if len(values[i]) < self.minDict[oid][i] else False for i in rangeMin]):
                    return S_ERROR(f"{err} values must be more then {', '.join(self.minDict[oid])}.")

            result = self.__fillX509Name(field, values)
            if not result["OK"]:
                return result

        return S_OK()

    def getProxy(self, userDN):
        """Generate user proxy

        :param str userDN: user DN

        :return: S_OK(str)/S_ERROR() -- contain a proxy string
        """
        self.__nameAttributes = []
        result = self.checkStatus(userDN)
        if result["OK"]:
            result = self.__createCertificate()
            if result["OK"]:
                certStr, keyStr = result["Value"]

                chain = X509Chain()
                result = chain.loadChainFromString(certStr)
                if result["OK"]:
                    result = chain.loadKeyFromString(keyStr)
                    if result["OK"]:
                        result = chain.generateProxyToString(365 * 24 * 3600)

        return result

    def generateDN(self, **kwargs):
        """Get DN of the user certificate that will be created

        :param dict kwargs: user description dictionary with possible fields:
               - FullName or CN
               - Email or emailAddress

        :return: S_OK(str)/S_ERROR() -- contain DN
        """
        if kwargs.get("FullName"):
            kwargs["CN"] = [kwargs["FullName"]]
        if kwargs.get("Email"):
            kwargs["emailAddress"] = [kwargs["Email"]]

        self.__nameAttributes = []
        self.log.info("Creating distinguished names chain")

        for oid in self.supplied:
            if oid not in [self.fields2oid[f] for f in self.dnList]:
                return S_ERROR(
                    'DNs order list does not contain supplied DN "%s"'
                    % self.oid2defField.get(oid, min(self.oid2fields[oid], key=len))
                )

        for field in self.dnList:
            values = []
            oid = self.fields2oid[field]
            if oid in self.match:
                for field in self.oid2fields[oid]:
                    if field in self.dnInfoDictCA:
                        values = self.dnInfoDictCA[field]
                if not values:
                    return S_ERROR(f'Not found "{field}" match DN in CA')
            for field in self.oid2fields[oid]:
                if kwargs.get(field):
                    values = kwargs[field] if isinstance(kwargs[field], list) else [kwargs[field]]
            if not values and oid in self.supplied:
                # Search default value
                if oid not in self.oid2defField:
                    return S_ERROR(f'No values set for "{min(self.oid2fields[oid], key=len)}" DN')
                values = self.parameters[oid]

            result = self.__fillX509Name(field, values)
            if not result["OK"]:
                return result

        # WARN: This logic not support list of distribtes name elements
        resDN = asn1_utils.nameToDN(x509.Name(self.__nameAttributes))

        result = self.checkStatus(resDN)
        if not result["OK"]:
            return result
        return S_OK(resDN)

    def __parseCACFG(self):
        """Parse CA configuration file"""
        block = ""
        self.cfg = {}
        self.supplied, self.optional, self.match, self.dnList = [], [], [], []
        with open(self.parameters["CAConfigFile"]) as caCFG:
            for line in caCFG:
                # Ignore comments
                line = re.sub(r"#.*", "", line)
                if re.findall(r"\[([A-Za-z0-9_]+)\]", line.replace(" ", "")):
                    block = "".join(re.findall(r"\[([A-Za-z0-9_]+)\]", line.replace(" ", "")))
                    if block not in self.cfg:
                        self.cfg[block] = {}
                if not block:
                    continue
                if len(re.findall("=", line)) == 1:
                    field, val = line.split("=")
                    field = field.strip()
                    variables = re.findall(r"[$]([A-Za-z0-9_]+)", val)
                    for v in variables:
                        for b in self.cfg:
                            if v in self.cfg[b]:
                                val = val.replace("$" + v, self.cfg[b][v])
                    if "default_ca" in self.cfg.get("ca", {}):
                        if "policy" in self.cfg.get(self.cfg["ca"]["default_ca"], {}):
                            if block == self.cfg[self.cfg["ca"]["default_ca"]]["policy"]:
                                self.dnList.append(field)
                    self.cfg[block][field] = val.strip()

        self.bits = int(self.cfg["req"].get("default_bits") or self.bits)
        self.algoritm = self.cfg[self.cfg["ca"]["default_ca"]].get("default_md") or self.algoritm
        if not self.parameters.get("CertFile"):
            self.parameters["CertFile"] = self.cfg[self.cfg["ca"]["default_ca"]]["certificate"]
            self.parameters["KeyFile"] = self.cfg[self.cfg["ca"]["default_ca"]]["private_key"]
        # Read distinguished names
        for k, v in self.cfg[self.cfg[self.cfg["ca"]["default_ca"]]["policy"]].items():
            oid = self.fields2oid[k]
            self.parameters[oid], self.minDict[oid], self.maxDict[oid] = [], [], []
            for k in [f"{i}.{k}" for i in range(0, 5)] + [k]:
                if k + "_default" in self.cfg["req"]["distinguished_name"]:
                    self.parameters[oid].append(self.cfg["req"]["distinguished_name"][k + "_default"])
                if k + "_min" in self.cfg["req"]["distinguished_name"]:
                    self.minDict[oid].append(self.cfg["req"]["distinguished_name"][k + "_min"])
                if k + "_max" in self.cfg["req"]["distinguished_name"]:
                    self.maxDict[oid].append(self.cfg["req"]["distinguished_name"][k + "_max"])
            if v == "supplied":
                self.supplied.append(oid)
            elif v == "optional":
                self.optional.append(oid)
            elif v == "match":
                self.match.append(oid)

    def __parseDN(self, dn):
        """Return DN fields

        :param str dn: DN

        :return: list -- contain tuple with positionOfField.fieldName, fieldOID, fieldValue
        """
        dnInfoDict = collections.OrderedDict()
        for f, v in [f.split("=") for f in dn.lstrip("/").split("/")]:
            if not v:
                return S_ERROR(f'No value set for "{f}"')
            if f not in dnInfoDict:
                dnInfoDict[f] = [v]
            else:
                dnInfoDict[f].append(v)
        return S_OK(dnInfoDict)

    def __fillX509Name(self, field, values):
        """Collect the DN attributes for the certificate subject

        :param str field: DN field name
        :param list values: values of field, order important

        :return: S_OK()/S_ERROR()
        """
        for value in values:
            if value:
                try:
                    self.__nameAttributes.append(
                        x509.NameAttribute(x509.ObjectIdentifier(self.fields2oid[field]), value)
                    )
                except (KeyError, TypeError, ValueError) as e:
                    return S_ERROR(f'Cannot set "{field}" field: {e!r}.')
        return S_OK()

    def __createCertificate(self):
        """Create new certificate for user

        :return: S_OK(tuple)/S_ERROR() -- tuple contain certificate and private key as strings
        """
        if self.algoritm not in _ALGORITHMS:
            return S_ERROR(f'Unsupported signing algorithm "{self.algoritm}"')
        hashAlgo = _ALGORITHMS[self.algoritm]()

        # Create user key pair
        userKey = rsa.generate_private_key(public_exponent=65537, key_size=self.bits)

        # Read CA certificate and key
        try:
            with open(self.parameters["CertFile"], "rb") as cf:
                caCert = x509.load_pem_x509_certificate(cf.read())
            with open(self.parameters["KeyFile"], "rb") as cf:
                caKey = serialization.load_pem_private_key(cf.read(), password=None)
        except Exception as e:
            return S_ERROR(f"Cannot load CA credentials: {e!r}")

        serial = 0
        while not serial:
            serial = secrets.randbits(64)

        now = datetime.datetime.now(datetime.timezone.utc)
        builder = (
            x509.CertificateBuilder()
            .subject_name(x509.Name(self.__nameAttributes))
            .issuer_name(caCert.subject)
            .public_key(userKey.public_key())
            .serial_number(serial)
            .not_valid_before(now)
            .not_valid_after(now + datetime.timedelta(days=400))
            .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=False)
            .add_extension(x509.ExtendedKeyUsage([x509.oid.ExtendedKeyUsageOID.CLIENT_AUTH]), critical=True)
        )

        try:
            userCert = builder.sign(caKey, hashAlgo)
        except Exception as e:
            return S_ERROR(f"Cannot sign the user certificate: {e!r}")

        userCertStr = userCert.public_bytes(serialization.Encoding.PEM).decode("ascii")
        userKeyStr = userKey.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        ).decode("ascii")
        return S_OK((userCertStr, userKeyStr))

    def _forceGenerateProxyForDN(self, dn, time, group=None):
        """An additional helper method for creating a proxy without any substantial validation,
        it can be used for a specific case(such as testing) where just need to generate a proxy
        with specific DN on the fly.

        :param str dn: requested proxy DN
        :param int time: expired time in a seconds
        :param str group: if need to add DIRAC group

        :return: S_OK(tuple)/S_ERROR() -- contain proxy as chain and as string
        """
        self.__nameAttributes = []
        result = self.__parseDN(dn)
        if not result["OK"]:
            return result
        dnInfoDict = result["Value"]

        for field, values in dnInfoDict.items():
            result = self.__fillX509Name(field, values)
            if not result["OK"]:
                return result

        result = self.__createCertificate()
        if result["OK"]:
            certStr, keyStr = result["Value"]
            chain = X509Chain()
            if chain.loadChainFromString(certStr)["OK"] and chain.loadKeyFromString(keyStr)["OK"]:
                result = chain.generateProxyToString(time, diracGroup=group)
        if not result["OK"]:
            return result
        chain = X509Chain()
        chain.loadProxyFromString(result["Value"])
        return S_OK((chain, result["Value"]))
