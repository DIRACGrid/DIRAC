"""
ElasticSearch wrapper
"""
import logging
from cmreslogging.handlers import CMRESHandler

from DIRAC.Resources.LogBackends.AbstractBackend import AbstractBackend


class ElasticSearchBackend(AbstractBackend):
    """
    ElasticsearchBackend is used to create an abstraction of the handler and the formatter concepts from logging.
    Here, we have a CMRESHandler which is part of an external library named 'cmreslogging' based on 'logging'.
    CMRESHandler is a specific handler created to send log records to an ElasticSearch DB. It does not need a Formatter
    object.
    """

    def __init__(self, backendParams=None):
        """
        CMRESHandler needs, at least, a hostname, a username, a password, a port and a specific index
        from the ElasticSearch DB to send log records.
        """
        # We give a format containing only asctime to add the field in elasticsearch
        # asctime is not created at the initialization of the LogRecords but built in the format process
        if not backendParams:
            backendParams = {}
        backendParams["Format"] = "%(asctime)s"

        super().__init__(CMRESHandler, logging.Formatter, backendParams)

    def _setHandlerParameters(self, backendParams=None):
        """
        Get the handler parameters from the backendParams.
        The keys of handlerParams should correspond to the parameter names of the associated handler.
        The method should be overridden in every backend that needs handler parameters.
        The method should be called before creating the handler object.

        :param dict parameters: parameters of the backend. ex: {'FileName': file.log}
        """
        # fixed parameters
        self._handlerParams["use_ssl"] = True
        self._handlerParams["verify_ssl"] = True
        self._handlerParams["auth_type"] = CMRESHandler.AuthType.NO_AUTH

        # variable parameters
        self._handlerParams["es_index_name"] = ""
        self._handlerParams["buffer_size"] = 1000
        self._handlerParams["flush_frequency_in_sec"] = 1
        user = None
        password = None
        host = ""
        port = 9203

        if backendParams is not None:
            self._handlerParams["es_index_name"] = backendParams.get("Index", self._handlerParams["es_index_name"])
            self._handlerParams["buffer_size"] = backendParams.get("BufferSize", self._handlerParams["buffer_size"])
            self._handlerParams["flush_frequency_in_sec"] = backendParams.get(
                "FlushTime", self._handlerParams["flush_frequency_in_sec"]
            )

            user = backendParams.get("User", user)
            password = backendParams.get("Password", password)
            if user is not None and password is not None:
                self._handlerParams["auth_type"] = CMRESHandler.AuthType.BASIC_AUTH
                self._handlerParams["auth_details"] = (user, password)

            host = backendParams.get("Host", host)
            port = int(backendParams.get("Port", port))

        self._handlerParams["hosts"] = [{"host": host, "port": port}]
