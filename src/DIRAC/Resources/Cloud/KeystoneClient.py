""" KeystoneClient class encapsulates the work with the keystone service interface
"""
import requests
import datetime

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities.TimeUtilities import fromString


class KeystoneClient:
    def __init__(self, url, parameters):
        self.log = gLogger.getSubLogger(self.__class__.__name__)
        self.url = url
        self.apiVersion = None
        if "v3" in url:
            self.apiVersion = 3
        if "v2" in url:
            self.apiVersion = 2
        if self.apiVersion is None:
            # Assume v2.0
            self.apiVersion = 2
            self.url = self.url.rstrip("/") + "/v2.0"
        self.parameters = parameters
        self.token = None
        self.expires = None
        self.project = self.parameters.get("Tenant", self.parameters.get("Project"))
        self.projectID = self.parameters.get("ProjectID")
        self.computeURL = None
        self.imageURL = None
        self.networkURL = None
        self.caPath = self.parameters.get("CAPath", True)

        self.valid = False
        result = self.initialize()
        if result["OK"]:
            self.valid = True
        else:
            gLogger.error("Keystone initialization failed: %s" % result["Message"])

    def initialize(self):
        """Initialize the Keystone object obtaining the corresponding token

        :return: S_OK/S_ERROR
        """

        self.log.debug("Initializing for API version %d" % self.apiVersion)

        result = self.getToken()
        if not result["OK"]:
            return result

        # If the tenant is not specified, try to get it and obtain the tenant specific token
        if not self.project:
            result = self.getTenants()
            if not result["OK"]:
                return result
            if result["Value"]:
                self.project, self.projectID = result["Value"][0]
                result = self.getToken(force=True)
                if not result["OK"]:
                    return result

        return S_OK()

    def getToken(self, force=False):
        """Get the Keystone token

        :param force: flag to force getting the token if even there is one in the cache
        :return: S_OK(token) or S_ERROR
        """

        if self.token is not None and not force:
            if self.expires and (self.expires - datetime.datetime.utcnow()).seconds > 300:
                return S_OK(self.token)

        if self.apiVersion == 2:
            result = self.__getToken2()
        else:
            result = self.__getToken3()
        return result

    def __getToken2(self):
        """Get the Keystone token for the version v2 of the keystone service

        :return: S_OK(token) or S_ERROR
        """

        user = self.parameters.get("User")
        password = self.parameters.get("Password")
        authArgs = {}
        if user and password:
            authDict = {"auth": {"passwordCredentials": {"username": user, "password": password}}}
            if self.project:
                authDict["auth"]["tenantName"] = self.project
        elif self.parameters.get("Auth") == "voms":
            authDict = {"auth": {"voms": True}}
            if self.project:
                authDict["auth"]["tenantName"] = self.project

            if self.parameters.get("Proxy"):
                authArgs["cert"] = self.parameters.get("Proxy")

        try:
            result = requests.post(
                "%s/tokens" % self.url,
                headers={"Content-Type": "application/json"},
                json=authDict,
                verify=self.caPath,
                **authArgs,
            )
        except Exception as exc:
            return S_ERROR("Exception getting keystone token: %s" % str(exc))

        output = result.json()

        if result.status_code in [400, 401]:
            message = "None"
            if "error" in output:
                message = output["error"].get("message")
            return S_ERROR("Authorization error: %s" % message)

        self.token = str(output["access"]["token"]["id"])
        expires = fromString(str(output["access"]["token"]["expires"]).replace("T", " ").replace("Z", ""))
        issued = fromString(str(output["access"]["token"]["issued_at"]).replace("T", " ").replace("Z", ""))
        self.expires = datetime.datetime.utcnow() + (expires - issued)

        self.projectID = output["access"]["token"]["tenant"]["id"]

        for endpoint in output["access"]["serviceCatalog"]:
            if endpoint["type"] == "compute":
                self.computeURL = str(endpoint["endpoints"][0]["publicURL"])
            elif endpoint["type"] == "image":
                self.imageURL = str(endpoint["endpoints"][0]["publicURL"])
            elif endpoint["type"] == "network":
                self.networkURL = str(endpoint["endpoints"][0]["publicURL"])
        return S_OK(self.token)

    def __getToken3(self):
        """Get the Keystone token for the version v3 of the keystone service

        :return: S_OK(token) or S_ERROR
        """

        domain = self.parameters.get("Domain", "Default")
        user = self.parameters.get("User")
        password = self.parameters.get("Password")
        appcred_file = self.parameters.get("Appcred")
        authDict = {}
        authArgs = {}
        if user and password:
            authDict = {
                "auth": {
                    "identity": {
                        "methods": ["password"],
                        "password": {"user": {"name": user, "domain": {"name": domain}, "password": password}},
                    }
                }
            }
        elif self.parameters.get("Auth") == "voms":
            authDict = {
                "auth": {
                    "identity": {
                        "methods": ["mapped"],
                        "mapped": {"voms": True, "identity_provider": "egi.eu", "protocol": "mapped"},
                    }
                }
            }
            if self.parameters.get("Proxy"):
                authArgs["cert"] = self.parameters.get("Proxy")
        elif appcred_file:
            # The application credentials are stored in a file of the format:
            # id secret
            ac_fd = open(appcred_file)
            auth_info = ac_fd.read()
            auth_info = auth_info.strip()
            ac_id, ac_secret = auth_info.split(" ", 1)
            ac_fd.close()
            authDict = {
                "auth": {
                    "identity": {
                        "methods": ["application_credential"],
                        "application_credential": {"id": ac_id, "secret": ac_secret},
                    }
                }
            }
        else:
            return S_ERROR("No valid credentials provided")

        # appcred includes the project scope binding in the credential itself
        if self.project and not appcred_file:
            authDict["auth"]["scope"] = {"project": {"domain": {"name": domain}, "name": self.project}}

        gLogger.debug(f"Request token with auth arguments: {str(authArgs)} and body {str(authDict)}")

        url = "%s/auth/tokens" % self.url
        try:
            result = requests.post(
                url,
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
                json=authDict,
                verify=self.caPath,
                **authArgs,
            )

        except Exception as exc:
            return S_ERROR("Exception getting keystone token: %s" % str(exc))

        if result.status_code not in [200, 201, 202, 203, 204]:
            return S_ERROR("Failed to get keystone token: %s" % result.text)

        try:
            self.token = result.headers["X-Subject-Token"]
        except Exception as exc:
            return S_ERROR("Failed to get keystone token: %s" % str(exc))

        output = result.json()

        expires = fromString(str(output["token"]["expires_at"]).replace("T", " ").replace("Z", ""))
        issued = fromString(str(output["token"]["issued_at"]).replace("T", " ").replace("Z", ""))
        self.expires = datetime.datetime.utcnow() + (expires - issued)

        if "project" in output["token"]:
            if output["token"]["project"]["name"] == self.project:
                self.projectID = output["token"]["project"]["id"]

        if "catalog" in output["token"]:
            for service in output["token"]["catalog"]:
                if service["type"] == "compute":
                    for endpoint in service["endpoints"]:
                        if endpoint["interface"] == "public":
                            self.computeURL = str(endpoint["url"])

                elif service["type"] == "image":
                    for endpoint in service["endpoints"]:
                        if endpoint["interface"] == "public":
                            self.imageURL = str(endpoint["url"])

                elif service["type"] == "network":
                    for endpoint in service["endpoints"]:
                        if endpoint["interface"] == "public":
                            self.networkURL = str(endpoint["url"])

        return S_OK(self.token)

    def getTenants(self):
        """Get available tenants for the current token

        :return: S_OK((tenant, tenant_id)) or S_ERROR
        """

        if self.token is None:
            return S_ERROR("No Keystone token yet available")

        try:
            result = requests.get(
                "%s/tenants" % self.url,
                headers={"Content-Type": "application/json", "X-Auth-Token": self.token},
                verify=self.caPath,
            )
        except Exception as exc:
            return S_ERROR("Failed to get keystone token: %s" % str(exc))

        if result.status_code != 200:
            return S_ERROR("Error: %s" % result.text)

        output = result.json()
        tenants = []
        if "tenants" in output:
            for item in output["tenants"]:
                tenants.append((item["name"], item["id"]))

        return S_OK(tenants)
