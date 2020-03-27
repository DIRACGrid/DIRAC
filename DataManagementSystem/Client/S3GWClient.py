""" Client to interact with the S3GW  """

from DIRAC.Core.Base.Client import Client, createClient


@createClient('DataManagement/S3GW')
class S3GWClient(Client):
  """ Client code to the S3GW
  """

  def __init__(self, url=None, **kwargs):
    """ Constructor function.
    """
    Client.__init__(self, **kwargs)
    self.setServer('DataManagement/S3GW')
    if url:
      self.setServer(url)

  def createPresignedUrl(self, storageName, s3_method, objectName, expiration=3600, **kwargs):
    """ Generate a presigned URL for a given object, given method, and given storage
        Permissions are checked against the DFC

        :param storageName: SE name
        :param s3_method: name of the S3 client method we want to perform.
        :param urls: urls
        :param expiration: duration of the token
    """
    return self._getRPC(**kwargs).createPresignedUrl(storageName, s3_method, objectName, expiration)
