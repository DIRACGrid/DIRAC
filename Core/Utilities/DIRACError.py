""" :mod: DIRACError
    ==========================

    .. module: DIRACError
    :synopsis: Error list and utilities for handling errors in DIRAC


    This module contains list of errors that can be encountered in DIRAC.
    It complements the errno module of python.

    It also contains utilities to manipulate these errors.

    Finaly, it contains a DIRACError class that contains an error number
    as well as a low level error message. It behaves like a string for
    compatibility reasons
"""

import os
import traceback


# To avoid conflict, the error numbers should be greater than 1000

ERRX = 1001
ERRY = 1002
EIMPERR = 1003
ENOMETH = 1004
EFILESIZE = 1005
ECONF = 1006
EGFAL = 1007
EBADCKS = 1008

# This translates the integer number into the name of the variable
dErrorCode = { 1001 : 'ERRX',
               1002 : 'ERRY',
               1003 : 'EIMPERR',
               1004 : 'ENOMETH',
               1005 : 'EFILESIZE',
               1006 : 'ECONF',
               1007 : 'EGFAL',
               1008 : 'EBADCKS',
                }


dStrError = { ERRX : "A human readable error message for ERRX",
              ERRY : "A nice message for ERRY",
              EIMPERR : "Failed to import library",
              ENOMETH : "No such method or function",
              EFILESIZE : "Bad file size",
              ECONF : "Configuration error",
              EGFAL : "Error with the gfal call",
              EBADCKS : "Bad checksum", }


# In case the error is returned as a string, and not as a DIRACError object, 
# these strings are used to test the error. 
compatErrorString = {
                     # ERRX : ['not found', 'X'],

                     }

def strerror(code):
  """ This method wraps up os.strerror, and behave the same way.
      It completes it with the DIRAC specific errors.
  """
  
  errMsg = "Unknown error %s"%code
  
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
        Checks whether 'other', which should be a string, is equal to the human readable form of the error msg
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



def S_ERROR_N( messageString = '' ):
  """ return value on error confition
  :param string messageString: error description
  """
  return { 'OK' : False, 'Message' :  messageString   }



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

  if type( inErr ) == str :  # old style
    # Create a DError object to represent the candidate
    derr = DError( candidate )
    return inErr == derr
  elif type( inErr ) == int:
    return inErr == candidate
  elif isinstance( inErr, DError ):
    return inErr.errno == candidate

  return False



