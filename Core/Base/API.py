""" DIRAC API Base Class """

from DIRAC                          import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Utilities.List      import sortList
from DIRAC.Core.Security.ProxyInfo  import getProxyInfo, formatProxyInfoAsString
from DIRAC.ConfigurationSystem.Client.Helpers.Registry         import getDNForUsername
from DIRAC.Core.Utilities.Version   import getCurrentVersion

import pprint, sys

COMPONENT_NAME = 'API'

def _printFormattedDictList( dictList, fields, uniqueField, orderBy ):
  """ Will print ordered the supplied field of a list of dictionaries """
  orderDict = {}
  fieldWidths = {}
  dictFields = {}
  for myDict in dictList:
    for field in fields:
      fieldValue = myDict[field]
      if not fieldWidths.has_key( field ):
        fieldWidths[field] = len( str( field ) )
      if len( str( fieldValue ) ) > fieldWidths[field]:
        fieldWidths[field] = len( str( fieldValue ) )
    orderValue = myDict[orderBy]
    if not orderDict.has_key( orderValue ):
      orderDict[orderValue] = []
    orderDict[orderValue].append( myDict[uniqueField] )
    dictFields[myDict[uniqueField]] = myDict
  headString = "%s" % fields[0].ljust( fieldWidths[fields[0]] + 5 )
  for field in fields[1:]:
    headString = "%s %s" % ( headString, field.ljust( fieldWidths[field] + 5 ) )
  print headString
  for orderValue in sortList( orderDict.keys() ):
    uniqueFields = orderDict[orderValue]
    for uniqueField in sortList( uniqueFields ):
      myDict = dictFields[uniqueField]
      outStr = "%s" % str( myDict[fields[0]] ).ljust( fieldWidths[fields[0]] + 5 )
      for field in fields[1:]:
        outStr = "%s %s" % ( outStr, str( myDict[field] ).ljust( fieldWidths[field] + 5 ) )
      print outStr


#TODO: some of these can just be functions, and moved out of here

class API( object ):
  """ An utilities class for APIs
  """

  #############################################################################

  def __init__( self ):
    """ c'tor
    """
    self._printFormattedDictList = _printFormattedDictList
    self.log = gLogger.getSubLogger( COMPONENT_NAME )
    self.section = COMPONENT_NAME
    self.pPrint = pprint.PrettyPrinter()
    #Global error dictionary
    self.errorDict = {}
    self.setup = gConfig.getValue( '/DIRAC/Setup', 'Unknown' )
    self.diracInfo = getCurrentVersion()['Value']

  #############################################################################

  def _errorReport( self, error, message = None ):
    """Internal function to return errors and exit with an S_ERROR() """
    if not message:
      message = error

    self.log.warn( error )
    return S_ERROR( message )

  #############################################################################

  def _prettyPrint( self, myObject ):
    """Helper function to pretty print an object. """
    print self.pPrint.pformat( myObject )

  #############################################################################

  def _getCurrentUser( self ):
    res = getProxyInfo( False, False )
    if not res['OK']:
      return self._errorReport( 'No proxy found in local environment', res['Message'] )
    proxyInfo = res['Value']
    gLogger.debug( formatProxyInfoAsString( proxyInfo ) )
    if not proxyInfo.has_key( 'group' ):
      return self._errorReport( 'Proxy information does not contain the group', res['Message'] )
    res = getDNForUsername( proxyInfo['username'] )
    if not res['OK']:
      return self._errorReport( 'Failed to get proxies for user', res['Message'] )
    return S_OK( proxyInfo['username'] )

  #############################################################################

  def _reportError( self, message, name = '', **kwargs ):
    """Internal Function. Gets caller method name and arguments, formats the
       information and adds an error to the global error dictionary to be
       returned to the user.
    """
    className = name
    if not name:
      className = __name__
    methodName = sys._getframe( 1 ).f_code.co_name
    arguments = []
    for key in kwargs:
      if kwargs[key]:
        arguments.append( '%s = %s ( %s )' % ( key, kwargs[key], type( kwargs[key] ) ) )
    finalReport = """Problem with %s.%s() call:
Arguments: %s
Message: %s
""" % ( className, methodName, '/'.join( arguments ), message )
    if self.errorDict.has_key( methodName ):
      tmp = self.errorDict[methodName]
      tmp.append( finalReport )
      self.errorDict[methodName] = tmp
    else:
      self.errorDict[methodName] = [finalReport]
    self.log.verbose( finalReport )
    return S_ERROR( finalReport )
