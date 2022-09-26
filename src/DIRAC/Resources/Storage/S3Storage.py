# https://docs.aws.amazon.com/AmazonS3/latest/API/API_Operations.html
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.list_objects
"""
Configuration of an S3 storage
Like others, but in protocol S3 add:

* SecureConnection: true if https, false otherwise
* Aws_access_key_id
* Aws_secret_access_key

if the Aws variables are not defined, it will try to go throught the S3Gateway

The key of the objects are the LFN without trailing path.
The Path should be the BucketName

"""
import copy
import errno
import functools

import os
import requests


import boto3
from botocore.exceptions import ClientError

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities.Adler import fileAdler
from DIRAC.Core.Utilities.DErrno import cmpError
from DIRAC.Core.Utilities.Pfn import pfnparse
from DIRAC.DataManagementSystem.Client.S3GatewayClient import S3GatewayClient
from DIRAC.Resources.Storage.StorageBase import StorageBase


LOG = gLogger.getSubLogger(__name__)


def _extractKeyFromS3Path(meth):
    """Decorator to split an s3 "external" url (s3://server:port/bucket/path)
    and return only the path part.
    """

    @functools.wraps(meth)
    def extractKey(self, urls, *args, **kwargs):

        # If set to False, we are already working with keys, so
        # skip all the splitting
        extractKeys = kwargs.pop("extractKeys", True)

        keysToUrls = {}
        keyArgs = {}

        successful = {}
        failed = {}

        if extractKeys:
            for url in urls:
                res = self._getKeyFromURL(url)  # pylint: disable=protected-access
                if not res["OK"]:
                    failed[url] = res["Message"]
                    continue

                key = res["Value"]
                keysToUrls[key] = url
                keyArgs[key] = urls[url]
        else:
            keyArgs = copy.copy(urls)

        result = meth(self, keyArgs, *args, **kwargs)

        if not result["OK"]:
            return result
        # Restore original paths

        for key in result["Value"]["Failed"]:
            failed[keysToUrls.get(key, key)] = result["Value"]["Failed"][key]
        for key in result["Value"]["Successful"]:
            successful[keysToUrls.get(key, key)] = result["Value"]["Successful"][key]

        result["Value"].update({"Successful": successful, "Failed": failed})
        return result

    return extractKey


class S3Storage(StorageBase):
    """
    .. class:: StorageBase

    """

    pluginName = "S3"

    _OUTPUT_PROTOCOLS = ["file", "s3", "http", "https"]

    def __init__(self, storageName, parameters):

        super().__init__(storageName, parameters)

        aws_access_key_id = parameters.get("Aws_access_key_id")
        aws_secret_access_key = parameters.get("Aws_secret_access_key")
        self.secureConnection = parameters.get("SecureConnection", "True") == "True"
        proto = "https" if self.secureConnection else "http"
        port = int(parameters.get("Port"))
        if not port:
            port = 443 if self.secureConnection else 80
        endpoint_url = "{}://{}:{}".format(proto, parameters["Host"], port)
        self.bucketName = parameters["Path"]

        self.s3_client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )

        self.srmSpecificParse = False
        self.pluginName = "S3"

        # if we have the credentials loaded, we can perform direct access
        # otherwise we have to go through the S3Gateway
        self.directAccess = aws_access_key_id and aws_secret_access_key
        self.S3GatewayClient = S3GatewayClient()

    def _getKeyFromURL(self, url):
        """Extract the Key from the URL.
        The key is basically the LFN without trailing slash

        I despise such path mangling, expecially after all the efforts to
        get ride of such method. However, since in the case of S3 we need
        to go back and forth between URL and LFN (for finding keys, checking
        accesses in the gw, etc), there is no other option...

        :param url: s3 url
        :returns: S_OK(key) / S_ERROR
        """

        res = pfnparse(url, srmSpecific=False)
        if not res["OK"]:
            return res

        splitURL = res["Value"]

        # The path originally looks like '/bucket/lhcb/user/c/chaen
        # We remove the trailing slash, and get the relative path
        # of bucket/lhcb/user/c/chaen starting from bucket,
        # which gives you basically the LFN without trailing slash
        path = os.path.relpath(splitURL["Path"].lstrip("/"), start=self.bucketName)

        key = os.path.join(path, splitURL["FileName"])

        return S_OK(key)

    # @_extractKeyFromS3Path
    # def direct_exists(self, keys):
    #   """ Check if the keys exists on the storage

    #   :param self: self reference
    #   :param keys: list of keys
    #   :returns: Failed dictionary: {pfn : error message}
    #             Successful dictionary: {pfn : bool}
    #             S_ERROR in case of argument problems
    #   """

    #   successful = {}
    #   failed = {}

    #   # If we have a direct access, we can just do the request directly
    #   if self.directAccess:
    #     for key in keys:
    #       try:
    #         self.s3_client.head_object(Bucket=self.bucketName, Key=key)
    #         successful[key] = True
    #       except ClientError as exp:
    #         if exp.response['Error']['Code'] == '404':
    #           successful[key] = False
    #         else:
    #           failed[key] = repr(exp)
    #       except Exception as exp:
    #         failed[key] = repr(exp)
    #   else:
    #     # Otherwise, ask the gw for a presigned URL,
    #     # and perform it with requests
    #     for key in keys:
    #       try:
    #         res = self.S3GatewayClient.createPresignedUrl(self.name, 'head_object', key)
    #         if not res['OK']:
    #           failed[key] = res['Message']
    #           continue
    #         presignedURL = res['Value']
    #         response = requests.get(presignedURL)
    #         if response.status_code == 200:
    #           successful[key] = True
    #         elif response.status_code == 404:  # not found
    #           successful[key] = False
    #         else:
    #           failed[key] = response.reason
    #       except Exception as e:
    #         failed[key] = repr(e)

    #   resDict = {'Failed': failed, 'Successful': successful}
    #   return S_OK(resDict)

    def exists(self, urls):
        """Check if the urls exists on the storage

        :param urls: list of URLs
        :returns: Failed dictionary: {url : error message}
                  Successful dictionary: {url : bool}
                  S_ERROR in case of argument problems
        """

        if self.directAccess:
            return self._direct_exists(urls)

        return self._presigned_exists(urls)

    @_extractKeyFromS3Path
    def _direct_exists(self, urls):
        """Check if the files exists on the storage

        :param urls: list of urls
        :returns: Failed dictionary: {pfn : error message}
                  Successful dictionary: {pfn : bool}
                  S_ERROR in case of argument problems
        """

        successful = {}
        failed = {}

        # the @_extractKeyFromS3Path transformed URL into keys
        keys = urls

        for key in keys:
            try:
                self.s3_client.head_object(Bucket=self.bucketName, Key=key)
                successful[key] = True
            except ClientError as exp:
                if exp.response["Error"]["Code"] == "404":
                    successful[key] = False
                else:
                    failed[key] = repr(exp)
            except Exception as exp:
                failed[key] = repr(exp)

        resDict = {"Failed": failed, "Successful": successful}
        return S_OK(resDict)

    def _presigned_exists(self, urls):
        """Check if the URLs exists on the storage

        :param urls: list of urls
        :returns: Failed dictionary: {pfn : error message}
                  Successful dictionary: {pfn : bool}
                  S_ERROR in case of argument problems
        """

        successful = {}
        failed = {}

        res = self.S3GatewayClient.createPresignedUrl(self.name, "head_object", urls)
        if not res["OK"]:
            return res

        failed.update(res["Value"]["Failed"])
        presignedURLs = res["Value"]["Successful"]

        # Otherwise, ask the gw for a presigned URL,
        # and perform it with requests
        for url, presignedURL in presignedURLs.items():
            try:
                response = requests.get(presignedURL)
                if response.status_code == 200:
                    successful[url] = True
                elif response.status_code == 404:  # not found
                    successful[url] = False
                else:
                    failed[url] = response.reason
            except Exception as e:
                failed[url] = repr(e)

        resDict = {"Failed": failed, "Successful": successful}
        return S_OK(resDict)

    def isFile(self, urls):
        """Check if the urls provided are a file or not

        In practice, if the object exists, it is necessarily a file

        :param urls: list of urls to be checked
        :returns: * Failed dict: {path : error message}
                  * Successful dict: {path : bool}
                  * S_ERROR in case of argument problems

        """

        return self.exists(urls)

    def getFile(self, urls, localPath=False):
        """Make a local copy of the urls.

        :param  urls: list of urls on the storage
        :param localPath: destination folder. Default is from current directory
        :returns: * Successful dict: {path : size}
                  * Failed dict: {path : errorMessage}
                  * S_ERROR in case of argument problems
        """

        if self.directAccess:
            return self._direct_getFile(urls, localPath=localPath)
        return self._presigned_getFile(urls, localPath=localPath)

    @_extractKeyFromS3Path
    def _direct_getFile(self, urls, localPath=False):
        """Make a local copy of the keys.

        :param  urls: list of urls  on storage
        :param localPath: destination folder. Default is from current directory
        :returns: * Successful dict: {path : size}
                  * Failed dict: {path : errorMessage}
                  * S_ERROR in case of argument problems
        """

        log = LOG.getSubLogger("getFile")

        # the @_extractKeyFromS3Path transformed URL into keys
        keys = urls

        failed = {}
        successful = {}

        for src_key in keys:
            try:
                fileName = os.path.basename(src_key)
                dest_file = os.path.join(localPath if localPath else os.getcwd(), fileName)
                log.debug(f"Trying to download {src_key} to {dest_file}")

                self.s3_client.download_file(self.bucketName, src_key, dest_file)

                successful[src_key] = os.path.getsize(dest_file)
            except Exception as exp:
                failed[src_key] = repr(exp)

        return S_OK({"Failed": failed, "Successful": successful})

    def _presigned_getFile(self, urls, localPath=False):
        """Make a local copy of the files.

        :param  urls: list of urls  on storage
        :param localPath: destination folder. Default is from current directory
        :returns: * Successful dict: {path : size}
                  * Failed dict: {path : errorMessage}
                  * S_ERROR in case of argument problems
        """

        log = LOG.getSubLogger("getFile")

        failed = {}
        successful = {}

        res = self.S3GatewayClient.createPresignedUrl(self.name, "get_object", urls)
        if not res["OK"]:
            return res

        failed.update(res["Value"]["Failed"])

        presignedURLs = res["Value"]["Successful"]

        for src_url, presignedURL in presignedURLs.items():
            try:
                fileName = os.path.basename(src_url)
                dest_file = os.path.join(localPath if localPath else os.getcwd(), fileName)
                log.debug(f"Trying to download {src_url} to {dest_file}")

                # Stream download to save memory
                # https://requests.readthedocs.io/en/latest/user/advanced/#body-content-workflow
                with requests.get(presignedURL, stream=True) as r:
                    r.raise_for_status()
                    with open(dest_file, "wb") as f:
                        for chunk in r.iter_content():
                            if chunk:  # filter out keep-alive new chuncks
                                f.write(chunk)

                successful[src_url] = os.path.getsize(dest_file)
            except Exception as exp:
                failed[src_url] = repr(exp)

        return S_OK({"Failed": failed, "Successful": successful})

    def putFile(self, urls, sourceSize=0):
        """Upload a local file.

        ..warning:: no 3rd party copy possible

        :param urls: dictionary { urls : localFile }
        :param sourceSize: size of the file in byte. Mandatory for third party copy (WHY ???)
                             Also, this parameter makes it essentially a non bulk operation for
                             third party copy, unless all files have the same size...
        :returns: * Successful dict: { path : size }
                  * Failed dict: { path : error message }
                  * S_ERROR in case of argument problems
        """
        if self.directAccess:
            return self._direct_putFile(urls, sourceSize=sourceSize)
        return self._presigned_putFile(urls, sourceSize=sourceSize)

    @_extractKeyFromS3Path
    def _direct_putFile(self, urls, sourceSize=0):
        """Upload a local file.

        ..warning:: no 3rd party copy possible

        :param urls: dictionary { urls : localFile }
        :param sourceSize: size of the file in byte. Mandatory for third party copy (WHY ???)
                             Also, this parameter makes it essentially a non bulk operation for
                             third party copy, unless all files have the same size...
        :returns: * Successful dict: { path : size }
                  * Failed dict: { path : error message }
                  * S_ERROR in case of argument problems
        """

        log = LOG.getSubLogger("putFile")

        # the @_extractKeyFromS3Path transformed URL into keys
        keys = urls

        failed = {}
        successful = {}

        for dest_key, src_file in keys.items():
            try:
                cks = fileAdler(src_file)
                if not cks:
                    log.warn("Cannot get ADLER32 checksum for %s" % src_file)

                with open(src_file, "rb") as src_fd:
                    self.s3_client.put_object(
                        Body=src_fd, Bucket=self.bucketName, Key=dest_key, Metadata={"Checksum": cks}
                    )

                successful[dest_key] = os.path.getsize(src_file)

            except Exception as e:
                failed[dest_key] = repr(e)

        return S_OK({"Failed": failed, "Successful": successful})

    def _presigned_putFile(self, urls, sourceSize=0):
        """Upload a local file.

        ..warning:: no 3rd party copy possible

        :param urls: dictionary { urls : localFile }
        :param sourceSize: size of the file in byte. Mandatory for third party copy (WHY ???)
                             Also, this parameter makes it essentially a non bulk operation for
                             third party copy, unless all files have the same size...
        :returns: * Successful dict: { path : size }
                  * Failed dict: { path : error message }
                  * S_ERROR in case of argument problems
        """

        log = LOG.getSubLogger("putFile")

        failed = {}
        successful = {}

        # Construct a dict <url:{x-amz-meta-checksum: adler32}>
        # it needs to be passed to createPresignedUrl
        urlAdlers = {url: {"x-amz-meta-checksum": fileAdler(src_file)} for url, src_file in urls.items()}

        res = self.S3GatewayClient.createPresignedUrl(self.name, "put_object", urlAdlers)
        if not res["OK"]:
            return res

        failed.update(res["Value"]["Failed"])

        # Contains <url: presignedResponse>
        presignedResponses = res["Value"]["Successful"]

        for dest_url, presignedResponse in presignedResponses.items():

            src_file = urls[dest_url]

            try:
                cks = fileAdler(src_file)
                if not cks:
                    log.warn("Cannot get ADLER32 checksum for %s" % src_file)

                presignedURL = presignedResponse["url"]
                presignedFields = presignedResponse["fields"]
                with open(src_file, "rb") as src_fd:
                    # files = {'file': (dest_key, src_fd)}
                    files = {"file": src_fd}
                    response = requests.post(presignedURL, data=presignedFields, files=files)

                    if not response.ok:
                        raise Exception(response.reason)

                successful[dest_url] = os.path.getsize(src_file)

            except Exception as e:
                failed[dest_url] = repr(e)

        return S_OK({"Failed": failed, "Successful": successful})

    def getFileMetadata(self, urls):
        """Get metadata associated to the file(s)

        :param  urls: list of urls on the storage
        :returns: * successful dict { path : metadata }
                  * failed dict { path : error message }
                  * S_ERROR in case of argument problems
        """
        if self.directAccess:
            return self._direct_getFileMetadata(urls)
        return self._presigned_getFileMetadata(urls)

    @_extractKeyFromS3Path
    def _direct_getFileMetadata(self, urls):
        """Get metadata associated to the file(s)

        :param  urls: list of urls on the storage
        :returns: * successful dict { path : metadata }
                  * failed dict { path : error message }
                  * S_ERROR in case of argument problems
        """

        # the @_extractKeyFromS3Path transformed URL into keys
        keys = urls

        failed = {}
        successful = {}

        for key in keys:
            try:
                response = self.s3_client.head_object(Bucket=self.bucketName, Key=key)
                responseMetadata = response["ResponseMetadata"]["HTTPHeaders"]

                metadataDict = self._addCommonMetadata(responseMetadata)
                metadataDict["File"] = True
                metadataDict["Size"] = int(metadataDict["content-length"])
                metadataDict["Checksum"] = metadataDict.get("x-amz-meta-checksum", "")

                successful[key] = metadataDict
            except Exception as exp:
                failed[key] = repr(exp)

        return S_OK({"Failed": failed, "Successful": successful})

    def _presigned_getFileMetadata(self, urls):
        """Get metadata associated to the file(s)

        :param  urls: list of urls on the storage
        :returns: * successful dict { path : metadata }
                  * failed dict { path : error message }
                  * S_ERROR in case of argument problems
        """

        failed = {}
        successful = {}

        res = self.S3GatewayClient.createPresignedUrl(self.name, "head_object", urls)
        if not res["OK"]:
            return res

        failed.update(res["Value"]["Failed"])

        presignedURLs = res["Value"]["Successful"]

        for url, presignedURL in presignedURLs.items():
            try:
                response = requests.head(presignedURL)
                if not response.ok:
                    raise Exception(response.reason)

                # Although the interesting fields are the same as when doing the query directly
                # the case is not quite the same, so make it lower everywhere
                responseMetadata = {headerKey.lower(): headerVal for headerKey, headerVal in response.headers.items()}

                metadataDict = self._addCommonMetadata(responseMetadata)
                metadataDict["File"] = True
                metadataDict["Size"] = int(metadataDict["content-length"])
                metadataDict["Checksum"] = metadataDict.get("x-amz-meta-checksum", "")

                successful[url] = metadataDict
            except Exception as exp:
                failed[url] = repr(exp)

        return S_OK({"Failed": failed, "Successful": successful})

    def removeFile(self, urls):
        """Physically remove the file specified by keys

        A non existing file will be considered as successfully removed

        :param urls: list of urls on the storage
        :returns: * Successful dict {path : True}
                  * Failed dict {path : error message}
                  * S_ERROR in case of argument problems
        """

        if self.directAccess:
            return self._direct_removeFile(urls)
        return self._presigned_removeFile(urls)

    @_extractKeyFromS3Path
    def _direct_removeFile(self, urls):
        """Physically remove the file specified by keys

        A non existing file will be considered as successfully removed

        :param urls: list of urls on the storage
        :returns: * Successful dict {path : True}
                  * Failed dict {path : error message}
                  * S_ERROR in case of argument problems
        """

        failed = {}
        successful = {}

        # the @_extractKeyFromS3Path transformed URL into keys
        keys = urls

        for key in keys:
            try:
                self.s3_client.delete_object(Bucket=self.bucketName, Key=key)
                successful[key] = True
            except Exception as exp:
                failed[key] = repr(exp)

        return S_OK({"Failed": failed, "Successful": successful})

    def _presigned_removeFile(self, urls):
        """Physically remove the file specified by keys

        A non existing file will be considered as successfully removed

        :param urls: list of urls on the storage
        :returns: * Successful dict {path : True}
                  * Failed dict {path : error message}
                  * S_ERROR in case of argument problems
        """

        failed = {}
        successful = {}

        res = self.S3GatewayClient.createPresignedUrl(self.name, "delete_object", urls)
        if not res["OK"]:
            return res

        failed.update(res["Value"]["Failed"])

        presignedURLs = res["Value"]["Successful"]

        for url, presignedURL in presignedURLs.items():
            try:
                response = requests.delete(presignedURL)
                if not response.ok:
                    raise Exception(response.reason)

                successful[url] = True
            except Exception as exp:
                failed[url] = repr(exp)

        return S_OK({"Failed": failed, "Successful": successful})

    def getFileSize(self, urls):
        """Get the physical size of the given file

        :param urls: list of urls on the storage
        :returns: * Successful dict {path : size}
                  * Failed dict {path : error message }
                  * S_ERROR in case of argument problem
        """

        res = self.getFileMetadata(urls)
        if not res["OK"]:
            return res

        failed = res["Value"]["Failed"]
        successful = {url: metadata["Size"] for url, metadata in res["Value"]["Successful"].items()}

        return S_OK({"Successful": successful, "Failed": failed})

    #############################################################
    #
    # These are the methods for directory manipulation
    #

    def createDirectory(self, urls):
        """Create directory on the storage.
            S3 does not have such a concept, but we return OK for everything

        :param urls: list of urls to be created on the storage
        :returns: Always Successful dict {path : True }
        """

        return S_OK({"Failed": {}, "Successful": {url: True for url in urls}})

    @staticmethod
    def notAvailable(*_args, **_kwargs):
        """Generic method for unavailable method on S3"""
        return S_ERROR("Functionality not available on S3")

    listDirectory = (
        isDirectory
    ) = getDirectory = removeDirectory = getDirectorySize = getDirectoryMetadata = putDirectory = notAvailable

    def getTransportURL(self, urls, protocols):
        """Get a transport URL for given urls
            If http/https is requested, the URLs will be valid for 24hours

        :param dict urls: s3 urls
        :param list protocols: a list of acceptable transport protocols in priority order.
                          In practice, besides 's3', it can only be:

                          * 'https' if secureConnection is True
                          * 'http' othewise

        :returns: succ/failed dict url with required protocol

        """

        res = super().getTransportURL(urls, protocols)
        # if the result is OK or the error different than errno.EPROTONOSUPPORT
        # we just return
        if not cmpError(res, errno.EPROTONOSUPPORT):
            return res

        # We support only http if it is an insecured connection and https if it is a secured connection
        if self.secureConnection and "https" not in protocols:
            return S_ERROR(errno.EPROTONOSUPPORT, "Only https protocol is supported")
        elif not self.secureConnection and "http" not in protocols:
            return S_ERROR(errno.EPROTONOSUPPORT, "Only http protocol is supported")

        # Make the presigned URLs valid for 24h
        if self.directAccess:
            return self.createPresignedUrl(urls, "get_object", expiration=60 * 60 * 24)

        return self.S3GatewayClient.createPresignedUrl(self.name, "get_object", urls, expiration=60 * 60 * 24)

    @_extractKeyFromS3Path
    def createPresignedUrl(self, urls, s3_method, expiration=3600):
        """Generate a presigned URL to share an S3 object

        :param urls: urls for which to generate a presigned URL. If s3_method is put_object, it must be a dict <url:Fields>
                      where fields are the metadata of the file
                      (see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.generate_presigned_post) # pylint: disable=line-too-long # noqa
        :param s3_method: name of the method for which to generate a presigned URL
        :param expiration: Time in seconds for the presigned URL to remain valid
        :return: Presigned URL as string. If error, returns None.
        """

        # the decorator transformed the urls into keys
        keys = urls

        successful = {}
        failed = {}

        # Generate a presigned URL for the S3 object
        log = LOG.getSubLogger("createPresignedUrl")

        for key in keys:
            try:
                if s3_method != "put_object":
                    response = self.s3_client.generate_presigned_url(
                        ClientMethod=s3_method, Params={"Bucket": self.bucketName, "Key": key}, ExpiresIn=expiration
                    )
                else:
                    fields = keys.get(key)
                    if not isinstance(fields, dict):
                        fields = None
                    response = self.s3_client.generate_presigned_post(
                        self.bucketName, key, Fields=fields, ExpiresIn=expiration
                    )

                successful[key] = response
            except ClientError as e:
                log.debug(e)
                failed[key] = repr(e)

        # The response contains the presigned URL
        return S_OK({"Successful": successful, "Failed": failed})
