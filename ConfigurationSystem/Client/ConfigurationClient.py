"""
  Custom TornadoClient for Configuration System
  Used like a normal client, should be instanciated if and only if we use the configuration service

  Because of limitation with JSON some datas are encoded in base64

"""

from base64 import b64encode, b64decode

from DIRAC.Core.Tornado.Client.TornadoClient import TornadoClient
from DIRAC.Core.Base.Client import Client


class CSJSONClient(TornadoClient):
  """
    The specific client for configuration system.
    To avoid JSON limitation the HTTPS handler encode data in base64
    before sending them, this class only decode the base64

    An exception is made with CommitNewData wich ENCODE in base64
  """

  def getCompressedData(self):
    """
      Transmit request to service and get data in base64,
      it decode base64 before returning

      :returns str:Configuration data, compressed
    """
    retVal = self.executeRPC('getCompressedData')
    if retVal['OK']:
      retVal['Value'] = b64decode(retVal['Value'])
    return retVal

  def getCompressedDataIfNewer(self, sClientVersion):
    """
      Transmit request to service and get data in base64,
      it decode base64 before returning.

      :returns str:Configuration data, if changed, compressed
    """
    retVal = self.executeRPC('getCompressedDataIfNewer', sClientVersion)
    if retVal['OK'] and 'data' in retVal['Value']:
      retVal['Value']['data'] = b64decode(retVal['Value']['data'])
    return retVal

  def commitNewData(self, sData):
    """
      Transmit request to service by encoding data in base64.

      :param: Data to commit, you may call this method with CSAPI and Modificator
    """
    return self.executeRPC('commitNewData', b64encode(sData))


class ConfigurationClient(Client):
  """
    Specific client to speak with ConfigurationServer.

    This class must contain at least the JSON decoder dedicated to
    the Configuration Server.

    You can implement more logic or function to the client here if needed,
    RPCCall can be made inside this class with executeRPC method.
  """

  # The JSON decoder for Configuration Server
  httpsClient = CSJSONClient

  def __init__(self, **kwargs):
    # By default we use Configuration/Server as url, client do the resolution
    # In some case url has to be static (when a slave register to the master server for example)
    # It's why we can use 'url' as keyword arguments
    if 'url' not in kwargs:
      kwargs['url'] = 'Configuration/Server'
    super(ConfigurationClient, self).__init__(**kwargs)
