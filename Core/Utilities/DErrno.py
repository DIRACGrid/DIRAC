""" :mod: DErrno
    ==========================

    .. module: DErrno
    :synopsis: Error list and utilities for handling errors in DIRAC


    This module contains list of errors that can be encountered in DIRAC.
    It complements the errno module of python.

    It also contains utilities to manipulate these errors.

    Finally, it contains a DErrno class that contains an error number
    as well as a low level error message. It behaves like a string for
    compatibility reasons

    In order to add extension specific error, you need to create in your extension the file
    Core/Utilities/DErrno.py, which will contain the following dictionary:
      * extra_dErrName: keys are the error name, values the number of it
      * extra_dErrorCode: same as dErrorCode. keys are the error code, values the name
                          (we don't simply revert the previous dict in case we do not
                           have a one to one mapping)
      * extra_dStrError: same as dStrError, Keys are the error code, values the error description
      * extra_compatErrorString: same as compatErrorString. The compatible error strings are
                                 added to the existing one, and not replacing them.


    Example of extension file :

      extra_dErrName = { 'ELHCBSPE' : 3001 }
      extra_dErrorCode = { 3001 : 'ELHCBSPE'}
      extra_dStrError = { 3001 : "This is a description text of the specific LHCb error" }
      extra_compatErrorString = { 3001 : ["living easy, living free"],
                             DErrno.ERRX : ['An error message for ERRX that is specific to LHCb']}

"""

import os
import traceback
import errno
import imp
import sys

# To avoid conflict, the error numbers should be greater than 1000
# We decided to group the by range of 100 per system

# 1000: Generic
# 1100: Core
# 1200: Framework
# 1300: Interfaces
# 1400: Config
# 1500: WMS / Workflow
# 1600: DMS/StorageManagement
# 1700: RMS
# 1800: Accounting
# 1900: TS
# 2000: RSS

# ## Generic (10XX)
# Python related: 0X
ETYPE = 1000
EIMPERR = 1001
ENOMETH = 1002
ECONF = 1003
# Files manipulation: 1X
ECTMPF = 1010
EOF = 1011
ERF = 1012
EWF = 1013
ESPF = 1014

# ## Core (11XX)
# Certificates and Proxy: 0X
EX509 = 1100
EPROXYFIND = 1101
EPROXYREAD = 1102
ECERTFIND = 1103
ECERTREAD = 1104
ENOCERT = 1105
ENOCHAIN = 1106
ENOPKEY = 1107
# DISET: 1X
EDISET = 1110
# 3rd party security: 2X
E3RDPARTY = 1120
EVOMS = 1121
# Databases : 3X
EDB = 1130
EMYSQL = 1131

# ## DMS/StorageManagement (16XX)
EFILESIZE = 1601
EGFAL = 1602
EBADCKS = 1603




# This translates the integer number into the name of the variable
dErrorCode = {
               # ## Generic (10XX)
               # 100X: Python related
               1000 : 'ETYPE',
               1001 : 'EIMPERR',
               1002 : 'ENOMETH',
               1003 : 'ECONF',
               # 101X: Files manipulation
               1010 : 'ECTMPF',
               1011 : 'EOF',
               1012 : 'ERF',
               1013 : 'EWF',
               1014 : 'ESPF',

               # ## Core
               # 110X: Certificates and Proxy
               1100 : 'EX509',
               1101 : 'EPROXYFIND',
               1102 : 'EPROXYREAD',
               1103 : 'ECERTFIND',
               1104 : 'ECERTREAD',
               1105 : 'ENOCERT',
               1106 : 'ENOCHAIN',
               1107 : 'ENOPKEY',
               # 111X: DISET
               1110 : 'EDISET',
               # 112X: 3rd party security
               1120 : 'E3RDPARTY',
               1121 : 'EVOMS',
               # 113X: Databases
               1130 : 'EDB',
               1131 : 'EMYSQL',

               # DMS/StorageManagement
               1601 : 'EFILESIZE',
               1602 : 'EGFAL',
               1603 : 'EBADCKS',
               }


dStrError = {
              # ## Generic (10XX)
              # 100X: Python related
              ETYPE : "Object Type Error",
              EIMPERR : "Failed to import library",
              ENOMETH : "No such method or function",
              ECONF : "Configuration error",
              # 101X: Files manipulation
              ECTMPF : "Failed to create temporary file",
              EOF : "Cannot open file",
              ERF : "Cannot read from file",
              EWF : "Cannot write to file",
              ESPF : "Cannot set permissions to file",

              # ## Core
              # 110X: Certificates and Proxy
              EX509 : "Generic Error with X509",
              EPROXYFIND : "Can't find proxy",
              EPROXYREAD : "Can't read proxy",
              ECERTFIND : "Can't find certificate",
              ECERTREAD : "Can't read certificate",
              ENOCERT : "No certificate loaded",
              ENOCHAIN : "No chain loaded",
              ENOPKEY : "No private key loaded",
              # 111X: DISET
              EDISET : "DISET Error",
              # 112X: 3rd party security
              E3RDPARTY: "3rd party security service error",
              EVOMS : "VOMS Error",
              # 113X: Databases
              EDB : "Database Error",
              EMYSQL : "MySQL Error",

              # DMS/StorageManagement
              EFILESIZE : "Bad file size",
              EGFAL : "Error with the gfal call",
              EBADCKS : "Bad checksum",
}


# In case the error is returned as a string, and not as a DErrno object, 
# these strings are used to test the error. 
compatErrorString = {
                     # ERRX : ['not found', 'X'],
                       errno.ENOENT : ['File does not exist']
                     }

def strerror(code):
  """ This method wraps up os.strerror, and behave the same way.
      It completes it with the DIRAC specific errors.
  """
  
  errMsg = "Unknown error %s" % code
  
  try:
    errMsg = dStrError[code]
  except KeyError:
    # It is not a DIRAC specific error, try the os one
    try:
      errMsg = os.strerror( code )
      # On some system, os.strerror raises an exception with unknown code,
      # on others, it returns a message...
    except ValueError:
      pass
  
  return errMsg


class DError( object ):
  """ This class is used to propagate errors through DIRAC.
      It contains a error code that should be one defined here or in errno python module.
      It also contains an error message which is not a human readable description, but the real
      low level technical message.
      Its interface is to be compatible with the one of a string in order to keep compatibility
      with the old error handling system
      
      CAUTION. The callstack attribute is used to print the sequence of events
      that lead to the error. It is set automatically in the __init__.
      It should be overwritten  only for serializing
  """
      

  def __init__( self, errno, errmsg = "" ):
    """ Initialize
        :param errno : error code
        :param errmsg : technical message
    """

    self.errno = errno
    self.errmsg = errmsg

    try:
      self._callStack = traceback.format_stack()
      self._callStack.pop()
    except:
      self._callStack = []

  def __repr__( self ):
    """ String representation """
    reprStr = "%s ( %s : %s)" % ( strerror( self.errno ), self.errno, self.errmsg )

    isVerbose = False
    stack = traceback.extract_stack()

    for filename, _linenb, function_name, _text in stack:
      if 'FrameworkSystem/private/logging/Logger.py' in filename:
        if function_name == 'debug':
          isVerbose = True
          break

    if isVerbose:
      reprStr += "\n" + "".join( self._callStack )

    return reprStr

  def __contains__( self, errorStr ):
    """ For compatibility reasons.
        Checks whether 'errorStr' is in the human readable form of the error msg or compat err msg
        errorStr has to be an str
    """
    # Check if the errorStr is in the standard message
    ret = ( errorStr in strerror( self.errno ) )
    if ret:
      return ret

    # If not, check whether the errorStr is in one of the compatibility error message
    ret = reduce( lambda x, y : x or ( errorStr in y ), compatErrorString.get( self.errno, [] ), False )

    return ret
  
  def __cmp__( self, errorStr ):
    """ For compatibility reasons.
        Checks whether 'errorStr', which should be a string, is equal to the human readable form of the error msg
    """
    # !!! Caution, if there is equality, we have to return 0 (rules of __cmp__)

    try:
      if errorStr == strerror( self.errno ):
        return 0
    except:
      pass

    if errorStr in compatErrorString.get( self.errno, [] ):
      return 0

    return 1

  def __getitem__( self, key ):
    """ Emulate the behavior of S_ERROR
    """
    if key == 'OK':
      return False
    elif key == 'Message':
      return "%s" % self
    raise KeyError( "{0} does not exist".format( key ) )
  
  def get(self, key, defaultValue = None):
    """ method like the "get" of a dictionary.
        Returns the value matching the key if exists,
        otherwise the default value

        :param key: item to lookup for
        :param defaultValue": if the key does not exist, return this value

        :return: the value matching the key or the default value
    """
    try:
      return self.__getitem__( key )
    except KeyError:
      return defaultValue



def cmpError( inErr, candidate ):
  """ This function compares an error (in its old form (a string...) or new (DError instance))
      with a candidate error code.

      :param inErr : a string, an integer, a DError instance
      :param candidate : error code to compare with

      :return True or False

      If a DError instance is passed, we compare the code with DError.errno
      If it is a Integer, we do a direct comparison
      If it is a String, we use compatErrorString and strerror to check the error string
  """

  if isinstance( inErr, basestring ) :  # old style
    # Create a DError object to represent the candidate
    derr = DError( candidate )
    return inErr == derr
  elif isinstance( inErr, dict ):  # if the S_ERROR structure is given
    # Create a DError object to represent the candidate
    derr = DError( candidate )
    return inErr.get( 'Message' ) == derr
  elif isinstance( inErr, int ):
    return inErr == candidate
  elif isinstance( inErr, DError ):
    return inErr.errno == candidate
  elif isinstance( inErr, dict ):  # S_ERROR object is given
    # Create a DError object to represent the candidate
    derr = DError( candidate )
    return inErr.get( 'Message', '' ) == derr
  else:
    raise TypeError( "Unknown input error type %s" % type( inErr ) )





def includeExtensionErrors():
  """ Merge all the errors of all the extensions into the errors of these modules
      Should be called only at the initialization of DIRAC, so by the parseCommandLine,
      dirac-agent.py, dirac-service.py, dirac-executor.py
  """

  def __recurseImport( modName, parentModule = None, fullName = False ):
    """ Internal function to load modules
    """
    if isinstance( modName, basestring ):
      modName = modName.split( "." )
    if not fullName:
      fullName = ".".join( modName )
    try:
      if parentModule:
        impData = imp.find_module( modName[0], parentModule.__path__ )
      else:
        impData = imp.find_module( modName[0] )
      impModule = imp.load_module( modName[0], *impData )
      if impData[0]:
        impData[0].close()
    except ImportError:
      return  None
    if len( modName ) == 1:
      return  impModule
    return __recurseImport( modName[1:], impModule, fullName = fullName )


  from DIRAC.ConfigurationSystem.Client.Helpers import CSGlobals
  allExtensions = CSGlobals.getCSExtensions()

  for extension in allExtensions:
    ext_derrno = None
    try:

      ext_derrno = __recurseImport( '%sDIRAC.Core.Utilities.DErrno' % extension )

      if ext_derrno:
        # The next 3 dictionary MUST be present for consistency

        # Global name of errors
        sys.modules[__name__].__dict__.update( ext_derrno.extra_dErrName )
        # Dictionary with the error codes
        sys.modules[__name__].dErrorCode.update( ext_derrno.extra_dErrorCode )
        # Error description string
        sys.modules[__name__].dStrError.update( ext_derrno.extra_dStrError )

        # extra_compatErrorString is optional
        for err in getattr( ext_derrno, 'extra_compatErrorString', [] ) :
          sys.modules[__name__].compatErrorString.setdefault( err, [] ).extend( ext_derrno.extra_compatErrorString[err] )

    except:
      pass

