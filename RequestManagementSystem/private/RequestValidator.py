########################################################################
# $HeadURL$
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

    There is a global singleton validator for general use defined in this module: gRequestValidator.

    If you need to extend this one with your own specific checks consider:

    * for adding Operation or Files required attributes use :addReqAttrsCheck: function::

    gRequestValidator.addReqAttrsCheck( "FooOperation", operationAttrs = [ "Bar", "Buzz"], filesAttrs = [ "LFN" ] )

    * for adding generic check define a new callable object ( function or functor ) which takes only one argument,
      say for functor::

    class MyValidator( RequestValidator ):

      @staticmethod
      def hasFoo( request ):
        if not request.Foo:
          return S_ERROR("Foo not set")
        return S_OK()

    or function::

    def hasBar( request ):
      if not request.Bar:
        return S_ERROR("Bar not set")
      return S_OK()

    and add this one to the validators set by calling gRequestValidator.addValidator, i.e.::

    gRequestValidator.addValidator( MyValidator.hasFoo )
    gRequestValidator.addValidator( hasFoo )

    Notice that all validators should always return S_ERROR/S_OK, no exceptions from that whatsoever!
"""
__RCSID__ = "$Id$"
# #
# @file RequestValidator.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/09/18 07:55:37
# @brief Definition of RequestValidator class.
# # import
import inspect
# # from DIRAC
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities.DIRACSingleton import DIRACSingleton

########################################################################
class RequestValidator( object ):
  """
  .. class:: RequestValidator

  This class validates newly created requests (before saving them in RequestDB) for
  required attributes.
  """
  # # one to rule them all
  __metaclass__ = DIRACSingleton

  # # dict with required attrs
  reqAttrs = { "ForwardDISET" : { "Operation": [ "Arguments" ], "Files" : [] },
               "PutAndRegister" : { "Operation" : [ "TargetSE" ], "Files" : [ "LFN", "PFN" ] },
               "ReplicateAndRegister" : { "Operation" : [ "TargetSE" ], "Files" : [ "LFN" ] },
               "PhysicalRemoval" : { "Operation" : ["TargetSE" ], "Files" : [ "PFN" ] },
               "RemoveFile" : { "Operation" : [], "Files" : [ "LFN" ] },
               "RemoveReplica" : { "Operation" : [ "TargetSE" ], "Files" : [ "LFN" ] },
               "ReTransfer" : { "Operation" : [ "TargetSE" ], "Files" : [ "LFN", "PFN" ] },
               "RegisterFile" : { "Operation" : [ ], "Files" : [ "LFN", "PFN", "ChecksumType", "Checksum", "GUID" ] },
               "RegisterReplica" : { "Operation" : [ "TargetSE" ], "Files" : [ "LFN", "PFN" ] } }

  def __init__( self ):
    """ c'tor

    just setting validation order
    """
    self.validator = ( self._hasRequestName,
                       self._hasOwner,
                       self._hasOperations,
                       self._hasType,
                       self._hasFiles,
                       self._hasRequiredAttrs,
                       self._hasChecksumAndChecksumType )

  @classmethod
  def addReqAttrsCheck( cls, operationType, operationAttrs = None, filesAttrs = None ):
    """ add required attributes of Operation of type :operationType:

    :param str operationType: Operation.Type
    :param list operationAttrs: required Operation attributes
    :param list filesAttrs: required Files attributes
    """
    toUpdate = { "Operation" : operationAttrs if operationAttrs else [],
                 "Files" : filesAttrs if filesAttrs else [] }
    if operationType not in cls.reqAttrs:
      cls.reqAttrs[operationType] = { "Operation" : [], "Files" : [] }
    for key, attrList in cls.reqAttrs[operationType].items():
      cls.reqAttrs[operationType][key] = list( set( attrList + toUpdate[key] ) )

  @classmethod
  def addValidator( cls, fcnObj ):
    """ add :fcnObj: validator """
    if not callable( fcnObj ):
      return S_ERROR( "supplied argument is not callable" )
    args = inspect.getargspec( fcnObj ).args
    if len( args ) not in ( 1, 2 ):
      return S_ERROR( "wrong number of arguments for supplied function object" )
    cls.validator = cls.validator + tuple( fcnObj, )
    return S_OK()

  def validate( self, request ):
    """ validation of a given :request:

    :param Request request: Request instance
    """
    for validator in self.validator:
      isValid = validator( request )
      if not isValid["OK"]:
        return isValid
    # # if we're here request is more or less valid
    return S_OK()

  @staticmethod
  def _hasDIRACSetup( request ):
    """ required attribute - DIRACSetup """
    if not request.DIRACSetup:
      return S_ERROR( "DIRACSetup not set" )
    return S_OK()

  @staticmethod
  def _hasOwner( request ):
    """ required attributes OwnerDn and OwnerGroup """
    if not request.OwnerDN:
      return S_ERROR( "Request '%s' is missing OwnerDN value" % request.RequestName )
    if not request.OwnerGroup:
      return S_ERROR( "Request '%s' is missing OwnerGroup value" % request.RequestName )
    return S_OK()

  @staticmethod
  def _hasRequestName( request ):
    """ required attribute: RequestName """
    if not request.RequestName:
      return S_ERROR( "RequestName not set" )
    return S_OK()

  @staticmethod
  def _hasOperations( request ):
    """ at least one operation is in """
    if not len( request ):
      return S_ERROR( "Operations not present in request '%s'" % request.RequestName )
    return S_OK()

  @staticmethod
  def _hasType( request ):
    """ operation type is set """
    for operation in request:
      if not operation.Type:
        return S_ERROR( "Operation #%d in request '%s' hasn't got Type set" % ( request.indexOf( operation ),
                                                                               request.RequestName ) )
    return S_OK()

  @classmethod
  def _hasFiles( cls, request ):
    """ check for files presence """
    for operation in request:
      if operation.Type not in cls.reqAttrs:
        return S_OK()
      if cls.reqAttrs[operation.Type]["Files"] and not len( operation ):
        return S_ERROR( "Operation #%d of type '%s' hasn't got files to process." % ( request.indexOf( operation ),
                                                                                      operation.Type ) )
      if not cls.reqAttrs[operation.Type]["Files"] and len( operation ):
        return S_ERROR( "Operation #%d of type '%s' has got files to process." % ( request.indexOf( operation ),
                                                                                   operation.Type ) )
    return S_OK()

  @classmethod
  def _hasRequiredAttrs( cls, request ):
    """ check required attributes for operations and files """
    for operation in request:
      if operation.Type in cls.reqAttrs:
        opAttrs = cls.reqAttrs[operation.Type]["Operation"]
        for opAttr in opAttrs:
          if not getattr( operation, opAttr ):
            return S_ERROR( "Operation #%d of type '%s' is missing %s attribute." % \
                             ( request.indexOf( operation ), operation.Type, opAttr ) )
        fileAttrs = cls.reqAttrs[operation.Type]["Files"]
        for opFile in operation:
          for fileAttr in fileAttrs:
            if not getattr( opFile, fileAttr ):
              return S_ERROR( "Operation #%d of type '%s' is missing %s attribute for file." % \
                               ( request.indexOf( operation ), operation.Type, fileAttr ) )
    return S_OK()

  @classmethod
  def _hasChecksumAndChecksumType( cls, request ):
    """ Checksum and ChecksumType should be specified """
    for operation in request:
      for opFile in operation:
        if any( [ opFile.Checksum, opFile.ChecksumType ] ) and not all( [opFile.Checksum, opFile.ChecksumType ] ):
          return S_ERROR( "File in operation #%d is missing Checksum (%s) or ChecksumType (%s)" % \
                          ( request.indexOf( operation ), opFile.Checksum, opFile.ChecksumType ) )
    return S_OK()

# # global instance
gRequestValidator = RequestValidator()
