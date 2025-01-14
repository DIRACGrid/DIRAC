""" ElasticDB is a base class used to connect an Opensearch database and manages queries.
"""
from DIRAC.ConfigurationSystem.Client.Utilities import getElasticDBParameters
from DIRAC.Core.Base.DIRACDB import DIRACDB
from DIRAC.Core.Utilities.ElasticSearchDB import ElasticSearchDB


class ElasticDB(DIRACDB, ElasticSearchDB):
    """Class for interfacing DIRAC ES DB definitions to ES clusters"""

    ########################################################################
    def __init__(self, fullName, indexPrefix="", parentLogger=None):
        """c'tor

        :param self: self reference
        :param str dbName: DIRAC name of the database for example: 'MonitoringDB'
        :param str fullName: The DIRAC full name of the database for example: 'Monitoring/MonitoringDB'
        :param str indexPrefix: it is the indexPrefix used to load all indexes
        :param parentLogger: logger to use as parentLogger
        """
        self.fullname = fullName

        result = getElasticDBParameters(fullName)
        if not result["OK"]:
            raise RuntimeError(f"Cannot get database parameters: {result['Message']}")

        dbParameters = result["Value"]
        self._dbHost = dbParameters["Host"]
        self._dbPort = dbParameters["Port"]
        self.__user = dbParameters["User"]
        self.__dbPassword = dbParameters["Password"]
        self.__useSSL = dbParameters.get("SSL", True)
        self.__useCRT = dbParameters.get("CRT", True)
        self.__ca_certs = dbParameters.get("ca_certs", None)
        self.__client_key = dbParameters.get("client_key", None)
        self.__client_cert = dbParameters.get("client_cert", None)

        super().__init__(
            host=self._dbHost,
            port=self._dbPort,
            user=self.__user,
            password=self.__dbPassword,
            indexPrefix=indexPrefix,
            useSSL=self.__useSSL,
            useCRT=self.__useCRT,
            ca_certs=self.__ca_certs,
            client_key=self.__client_key,
            client_cert=self.__client_cert,
            parentLogger=parentLogger,
        )

        if not self._connected:
            raise RuntimeError(f"Can not connect to ES cluster {self.clusterName}, exiting...")

        self.log.debug("================= OpenSearch ==================")
        self.log.debug(f"Host: {self._dbHost} ")
        if self._dbPort:
            self.log.debug("Port: %d " % self._dbPort)
        else:
            self.log.debug("Port: Not specified, assuming URL points to right location")
        self.log.debug(
            "Connecting with %s, %s:%s"
            % (
                "SSL" if self.__useSSL else "no SSL",
                self.__user if self.__user else "no user",
                "with password" if self.__dbPassword else "no password",
            )
        )
        self.log.debug(f"ClusterName: {self.clusterName}   ")
        self.log.debug("==================================================")
