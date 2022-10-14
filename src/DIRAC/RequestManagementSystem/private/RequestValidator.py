########################################################################
# File: RequestValidator.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2012/09/18 07:55:16
########################################################################
""" :mod: RequestValidator

    ======================

    .. module: RequestValidator

    :synopsis: request validator

    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    A general and simple request validator checking for required attributes and logic.
    It checks if required attributes are set/unset but not for their values.

    RequestValidator class implements the DIRACSingleton pattern, no global object is
    required to keep a single instance.

    If you need to extend this one with your own specific checks consider:

      * for adding Operation or Files required attributes use :any:`addReqAttrsCheck` function::

          RequestValidator().addReqAttrsCheck( "FooOperation", operationAttrs = [ "Bar", "Buzz"], filesAttrs = [ "LFN" ] )

      * for adding generic check define a new callable object ( function or functor ) which takes only one argument,
        say for functor::

          class MyValidator( RequestValidator ):

            @staticmethod
            def hasFoo( request ):
              if not request.Foo:
                return S_ERROR("Foo not set")
              return S_OK()

      * or function::

          def hasBar( request ):
            if not request.Bar:
              return S_ERROR("Bar not set")
            return S_OK()

    and add this one to the validators set by calling `RequestValidator().addValidator`, i.e.::

      RequestValidator().addValidator( MyValidator.hasFoo )
      RequestValidator().addValidator( hasFoo )

    Notice that all validators should always return S_ERROR/S_OK, no exceptions from that whatsoever!
"""

import inspect

# # from DIRAC
from DIRAC import S_OK, S_ERROR, gConfig, gLogger
from DIRAC.Core.Security.Properties import FULL_DELEGATION, LIMITED_DELEGATION
from DIRAC.Core.Utilities.DIRACSingleton import DIRACSingleton
from DIRAC.ConfigurationSystem.Client import PathFinder


class RequestValidator(metaclass=DIRACSingleton):
    """
    .. class:: RequestValidator

    This class validates newly created requests (before saving them in RequestDB) for
    required attributes.
    """

    # # dict with required attrs
    reqAttrs = {
        "ForwardDISET": {"Operation": ["Arguments"], "Files": []},
        "PutAndRegister": {"Operation": ["TargetSE"], "Files": ["LFN", "PFN"]},
        "ReplicateAndRegister": {"Operation": ["TargetSE"], "Files": ["LFN"]},
        "PhysicalRemoval": {"Operation": ["TargetSE"], "Files": ["LFN"]},
        "RemoveFile": {"Operation": [], "Files": ["LFN"]},
        "RemoveReplica": {"Operation": ["TargetSE"], "Files": ["LFN"]},
        "ReTransfer": {
            "Operation": ["TargetSE"],
            "Files": ["LFN", "PFN"],
        },
        "RegisterFile": {
            "Operation": [],
            "Files": ["LFN", "PFN", "ChecksumType", "Checksum", "GUID"],
        },
        "RegisterReplica": {
            "Operation": ["TargetSE"],
            "Files": ["LFN", "PFN"],
        },
    }

    # All the operationHandlers defined in the CS
    opHandlers = set()

    def __init__(self):
        """c'tor

        just setting validation order
        """
        self.validator = (
            self._hasRequestName,
            self._hasOwner,
            self._hasOperations,
            self._hasType,
            self._hasFiles,
            self._hasRequiredAttrs,
            self._hasChecksumAndChecksumType,
        )

        configPath = PathFinder.getAgentSection("RequestManagement/RequestExecutingAgent")

        # # operation handlers over here
        opHandlersPath = "{}/{}".format(configPath, "OperationHandlers")
        opHandlers = gConfig.getSections(opHandlersPath)
        if not opHandlers["OK"]:
            gLogger.error(opHandlers["Message"])
        else:
            self.opHandlers = set(opHandlers["Value"])

    @classmethod
    def addReqAttrsCheck(cls, operationType, operationAttrs=None, filesAttrs=None):
        """add required attributes of Operation of type :operationType:

        :param str operationType: Operation.Type
        :param operationAttrs: required Operation attributes
        :type operationAttrs: python:list
        :param filesAttrs: required Files attributes
        :type filesAttrs: python:list
        """
        toUpdate = {"Operation": operationAttrs if operationAttrs else [], "Files": filesAttrs if filesAttrs else []}
        if operationType not in cls.reqAttrs:
            cls.reqAttrs[operationType] = {"Operation": [], "Files": []}
        for key, attrList in cls.reqAttrs[operationType].items():
            cls.reqAttrs[operationType][key] = list(set(attrList + toUpdate[key]))

    @classmethod
    def addValidator(cls, fcnObj):
        """add `fcnObj` validator"""
        if not callable(fcnObj):
            return S_ERROR("supplied argument is not callable")
        args = inspect.getargspec(fcnObj).args
        if len(args) not in (1, 2):
            return S_ERROR("wrong number of arguments for supplied function object")
        cls.validator = cls.validator + tuple(
            fcnObj,
        )
        return S_OK()

    def validate(self, request):
        """validation of a given `request`

        :param ~Request.Request request: Request instance
        """
        for validator in self.validator:
            isValid = validator(request)
            if not isValid["OK"]:
                return isValid
        # # if we're here request is more or less valid
        return S_OK()

    @staticmethod
    def _hasOwner(request):
        """required attributes OwnerDn and OwnerGroup"""
        if not request.OwnerDN:
            return S_ERROR("Request '%s' is missing OwnerDN value" % request.RequestName)
        if not request.OwnerGroup:
            return S_ERROR("Request '%s' is missing OwnerGroup value" % request.RequestName)
        return S_OK()

    @staticmethod
    def _hasRequestName(request):
        """required attribute: RequestName"""
        if not request.RequestName:
            return S_ERROR("RequestName not set")
        return S_OK()

    @staticmethod
    def _hasOperations(request):
        """at least one operation is in"""
        if not len(request):
            return S_ERROR("Operations not present in request '%s'" % request.RequestName)
        return S_OK()

    @staticmethod
    def _hasType(request):
        """operation type is set"""
        for operation in request:
            if not operation.Type:
                return S_ERROR(
                    "Operation #%d in request '%s' hasn't got Type set"
                    % (request.indexOf(operation), request.RequestName)
                )
        return S_OK()

    @classmethod
    def _hasFiles(cls, request):
        """check for files presence"""
        for operation in request:
            if operation.Type not in cls.reqAttrs:
                return S_OK()
            if cls.reqAttrs[operation.Type]["Files"] and not len(operation):
                return S_ERROR(
                    "Operation #%d of type '%s' hasn't got files to process."
                    % (request.indexOf(operation), operation.Type)
                )
            if not cls.reqAttrs[operation.Type]["Files"] and len(operation):
                return S_ERROR(
                    "Operation #%d of type '%s' has got files to process."
                    % (request.indexOf(operation), operation.Type)
                )
        return S_OK()

    @classmethod
    def _hasRequiredAttrs(cls, request):
        """check required attributes for operations and files"""
        for operation in request:
            if operation.Type in cls.reqAttrs:
                opAttrs = cls.reqAttrs[operation.Type]["Operation"]
                for opAttr in opAttrs:
                    if not getattr(operation, opAttr):
                        return S_ERROR(
                            "Operation #%d of type '%s' is missing %s attribute."
                            % (request.indexOf(operation), operation.Type, opAttr)
                        )
                fileAttrs = cls.reqAttrs[operation.Type]["Files"]
                for opFile in operation:
                    for fileAttr in fileAttrs:
                        if not getattr(opFile, fileAttr):
                            return S_ERROR(
                                "Operation #%d of type '%s' is missing %s attribute for file."
                                % (request.indexOf(operation), operation.Type, fileAttr)
                            )
        return S_OK()

    @classmethod
    def _hasChecksumAndChecksumType(cls, request):
        """Checksum and ChecksumType should be specified"""
        for operation in request:
            for opFile in operation:
                if any([opFile.Checksum, opFile.ChecksumType]) and not all([opFile.Checksum, opFile.ChecksumType]):
                    return S_ERROR(
                        "File in operation #%d is missing Checksum (%s) or ChecksumType (%s)"
                        % (request.indexOf(operation), opFile.Checksum, opFile.ChecksumType)
                    )
        return S_OK()

    def _hasExistingOperationTypes(self, request):
        """Check that there is a handler defined in the CS for each operation type"""
        requiredHandlers = {op.Type for op in request}
        nonExistingHandlers = requiredHandlers - self.opHandlers

        if nonExistingHandlers:
            return S_ERROR(
                "The following operation type(s) have no handlers defined in the CS: %s" % nonExistingHandlers
            )

        return S_OK()

    @staticmethod
    def setAndCheckRequestOwner(request, remoteCredentials):
        """
        CAUTION: meant to be called on the server side.
                (does not make much sense otherwise)

        Sets the ownerDN and ownerGroup of the Request from
        the client's credentials.

        If they are already set, make sure the client is allowed to do so
        (FULL_DELEGATION or LIMITED_DELEGATION). This is the case of pilots or
        the RequestExecutingAgent

        :param request: the request to test
        :param remoteCredentials: credentials from the clients

        :returns: True if everything is fine, False otherwise
        """

        credDN = remoteCredentials["DN"]
        credGroup = remoteCredentials["group"]
        credProperties = remoteCredentials["properties"]

        # If the owner or the group was not set, we use the one of the credentials
        if not request.OwnerDN or not request.OwnerGroup:
            request.OwnerDN = credDN
            request.OwnerGroup = credGroup
            return True

        # From here onward, we expect the ownerDN/group to already have a value

        # If the credentials in the Request match those from the credentials, it's OK
        if request.OwnerDN == credDN and request.OwnerGroup == credGroup:
            return True

        # From here, something/someone is putting a request on behalf of someone else

        # Only allow this if the credentials have Full or Limited delegation properties

        if FULL_DELEGATION in credProperties or LIMITED_DELEGATION in credProperties:
            return True

        return False
