""" Client to interact with the S3Gateway  """
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from DIRAC.Core.Base.Client import Client, createClient


@createClient('DataManagement/S3Gateway')
class S3GatewayClient(Client):
  """ Client code to the S3Gateway
  """

  def __init__(self, url=None, **kwargs):
    """ Constructor function.
    """
    super(S3GatewayClient, self).__init__(**kwargs)
    self.setServer('DataManagement/S3Gateway')
    if url:
      self.setServer(url)

  def createPresignedUrl(self, storageName, s3_method, urls, expiration=3600, **kwargs):
    """ Generate a presigned URL for a given object, given method, and given storage
        Permissions are checked against the DFC

        :param storageName: SE name
        :param s3_method: name of the S3 client method we want to perform.
        :param urls: Iterable of urls. If s3_method is put_object, it must be a dict <url:fields> where fields
                     is a dictionary (see ~DIRAC.Resources.Storage.S3Storage.S3Storage.createPresignedUrl)
        :param expiration: duration of the token
    """

    return self._getRPC(**kwargs).createPresignedUrl(storageName, s3_method, urls, expiration)
