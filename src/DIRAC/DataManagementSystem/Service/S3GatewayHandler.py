"""
Service handler for generating pre-signed URLs for S3 storages.
Permissions to request a URL for a given action are mapped against FC permissions
This service can serve presigned URL for any S3 storage it has the credentials for.


.. literalinclude:: ../ConfigTemplate.cfg
  :start-after: ##BEGIN S3Gateway
  :end-before: ##END
  :dedent: 2
  :caption: S3Gateway options

"""
import errno

# from DIRAC
from DIRAC import S_ERROR, S_OK, gLogger
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.DISET.ThreadConfig import ThreadConfig
from DIRAC.Core.Utilities.ReturnValues import returnSingleResult
from DIRAC.DataManagementSystem.Utilities.DMSHelpers import DMSHelpers
from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
from DIRAC.Resources.Storage.StorageElement import StorageElement

########################################################################

LOG = gLogger.getSubLogger(__name__)


class S3GatewayHandlerMixin:
    """
    .. class:: S3GatewayHandler

    """

    # FC instance to check whether access is permitted or not
    _fc = None

    # Mapping between the S3 methods and the DFC methods
    _s3ToFC_methods = {
        "head_object": "getFileMetadata",
        "get_object": "getFileMetadata",  # consider that if we are allowed to see the file metadata
        # we can also download it
        "put_object": "addFile",
        "delete_object": "removeFile",
    }

    _S3Storages = {}

    # This allows us to perform the DFC queries on behalf of a user
    # without having to recreate a DFC object every time and
    # pass it the "delegatedDN" and "delegatedGroup" values
    _tc = ThreadConfig()

    @classmethod
    def initializeHandler(cls, serviceInfoDict):
        """initialize handler"""

        log = LOG.getSubLogger("initializeHandler")

        for seName in DMSHelpers().getStorageElements():
            se = StorageElement(seName)
            # TODO: once we finally merge _allProtocolParameters with the
            # standard paramaters in the StorageBase, this will be much neater

            for storagePlugin in se.storages.values():
                storageParam = storagePlugin._allProtocolParameters  # pylint: disable=protected-access

                if (
                    storageParam.get("Protocol") == "s3"
                    and "Aws_access_key_id" in storageParam
                    and "Aws_secret_access_key" in storageParam
                ):

                    cls._S3Storages[seName] = storagePlugin
                    log.debug("Add %s to the list of usable S3 storages" % seName)
                    break

        log.info("S3Gateway initialized storages", "%s" % list(cls._S3Storages))

        cls._fc = FileCatalog()

        return S_OK()

    def _hasAccess(self, lfn, s3_method):
        """Check if we have permission to execute given operation on the given file (if exists) or its directory"""

        opType = self._s3ToFC_methods.get(s3_method)
        if not opType:
            return S_ERROR(errno.EINVAL, "Unknown S3 method %s" % s3_method)

        return returnSingleResult(self._fc.hasAccess(lfn, opType))

    types_createPresignedUrl = [str, str, (dict, list), int]

    def export_createPresignedUrl(self, storageName, s3_method, urls, expiration):
        """Generate a presigned URL for a given object, given method, and given storage
        Permissions are checked against the DFC

        :param storageName: SE name
        :param s3_method: name of the S3 client method we want to perform.
        :param urls: Iterable of urls. If s3_method is put_object, it must be a dict <url:fields> where fields
                     is a dictionary (see ~DIRAC.Resources.Storage.S3Storage.S3Storage.createPresignedUrl)
        :param expiration: duration of the token
        """

        log = LOG.getSubLogger("createPresignedUrl")

        if s3_method == "put_object" and not isinstance(urls, dict):
            return S_ERROR(errno.EINVAL, "urls has to be a dict <url:fields>")

        # Fetch the remote credentials, and set them in the ThreadConfig
        # This allows to perform the FC operations on behalf of the user
        credDict = self.getRemoteCredentials()
        if not credDict:
            # If we can't obtain remote credentials, consider it permission denied
            return S_ERROR(errno.EACCES, "Could not obtain remote credentials")

        self._tc.setDN(credDict["DN"])
        self._tc.setGroup(credDict["group"])

        successful = {}
        failed = {}
        s3Plugin = self._S3Storages[storageName]
        for url in urls:
            try:
                log.verbose(
                    "Creating presigned URL",
                    f"SE: {storageName} Method: {s3_method} URL: {url} Expiration: {expiration}",
                )

                # Finding the LFN to query the FC
                # I absolutely hate doing such path mangling but well....
                res = s3Plugin._getKeyFromURL(url)  # pylint: disable=protected-access
                if not res["OK"]:
                    failed[url] = res["Message"]
                    log.debug(f"Could not parse the url {url} {res}")
                    continue

                lfn = "/" + res["Value"]

                log.debug(f"URL: {url} -> LFN {lfn}")

                # Checking whether access is permitted
                res = self._hasAccess(lfn, s3_method)
                if not res["OK"]:
                    failed[url] = res["Message"]
                    continue

                if not res["Value"]:
                    failed[url] = "Permission denied"
                    continue

                res = returnSingleResult(
                    s3Plugin.createPresignedUrl({url: urls.get("Fields")}, s3_method, expiration=expiration)
                )

                log.debug(f"Presigned URL for {url}: {res}")
                if res["OK"]:
                    successful[url] = res["Value"]
                else:
                    failed["url"] = res["Message"]
            except Exception as e:
                log.exception("Exception presigning URL")
                failed[url] = repr(e)

        return S_OK({"Successful": successful, "Failed": failed})


class S3GatewayHandler(S3GatewayHandlerMixin, RequestHandler):
    pass
