"""
ElasticSearch wrapper
"""

__RCSID__ = "$Id$"

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

  def __init__(self):
    """
    CMRESHandler needs, at least, a hostname, a username, a password, a port and a specific index
    from the ElasticSearch DB to send log records.
    """
    super(ElasticSearchBackend, self).__init__(None, None)
    self.__host = ''
    self.__user = None
    self.__passwd = None
    self.__port = 9203
    self.__index = ''
    self.__bufferSize = 1000
    self.__flushTime = 1

  def createHandler(self, parameters=None):
    """
    Each backend can initialize its attributes and create its handler with them.

    :params parameters: dictionary of parameters. ex: {'FileName': file.log}
    """
    if parameters is not None:
      self.__host = parameters.get('Host', self.__host)
      self.__user = parameters.get('User', self.__user)
      self.__passwd = parameters.get('Password', self.__passwd)
      self.__port = int(parameters.get('Port', self.__port))
      self.__index = parameters.get('Index', self.__index)
      self.__bufferSize = int(parameters.get('BufferSize', self.__bufferSize))
      self.__flushTime = int(parameters.get('FlushTime', self.__flushTime))

    if self.__user is not None and self.__passwd is not None:
      self._handler = CMRESHandler(hosts=[{'host': self.__host, 'port': self.__port}],
                                   auth_type=CMRESHandler.AuthType.BASIC_AUTH,
                                   auth_details=(self.__user, self.__passwd),
                                   es_index_name=self.__index,
                                   use_ssl=True,
                                   verify_ssl=True,
                                   buffer_size=self.__bufferSize,
                                   flush_frequency_in_sec=self.__flushTime)
    else:
      self._handler = CMRESHandler(hosts=[{'host': self.__host, 'port': self.__port}],
                                   auth_type=CMRESHandler.AuthType.NO_AUTH,
                                   es_index_name=self.__index,
                                   use_ssl=True,
                                   verify_ssl=True,
                                   buffer_size=self.__bufferSize,
                                   flush_frequency_in_sec=self.__flushTime)
    # We give a format containing only asctime to add the field in elasticsearch
    # asctime is not created at the initialization of the LogRecords but built in the format process
    self._handler.setFormatter(logging.Formatter('%(asctime)s'))

  def setLevel(self, level):
    """
    No possibility to set the level of the ElasticSearch handler.
    It is not set by default so it can send all Log Records of all levels to ElasticSearch.
    """
    pass

  def setFormat(self, fmt, datefmt, options):
    """
    Each backend give a format to their formatters and attach them to their handlers.

    :params fmt: string representing the log format
    :params datefmt: string representing the date format
    :params component: string represented as "system/component"
    :params options: dictionary of logging options. ex: {'Color': True}
    """
    pass
