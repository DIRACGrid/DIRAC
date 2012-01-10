
import types
from DIRAC import S_OK, S_ERROR, gConfig
from DIRAC.ConfigurationSystem.Client.PathFinder import getAgentSection
from DIRAC.Core.Utilities.CFG import CFG
from DIRAC.Core.Utilities import List
from DIRAC.Core.Utilities.JDL import loadJDLAsCFG, dumpCFGAsJDL


class JobState( object ):

  class TracedMethod( object ):

    def __init__( self, functor ):
      self.__functor = functor

    #Black magic to map the unbound function received at TracedMethod.__init__ time
    #to a JobState method with a proper self
    def __get__( self, obj, type = None ):
      return self.__class__( self.__func.__get__( obj, type ) )

    def __call__( self, *args, **kwargs ):
      funcSelf = self.__functor.__self__
      if funcSelf.traceActions:
        if kwargs:
          trace = ( self.__functor.__name__, args, kwargs )
        else:
          trace = ( self.__functor.__name__, args )
        funcSelf.addActionToTrace( trace )

      return self.__functor( *args, **kwargs )

  def __init__( self, jid, keepTrace = False, syncDB = True ):
    self.__jid = jid
    self.__description = CFG()
    self.__descDirty = False
    self.__keepTrace = keepTrace
    self.__syncDB = False
    self.__useCache = False
    self.__changes = []
    self.__dataCache = {}

  def __cacheData( self, key, value ):
    self.__dataCache[ key ] = value

  def __getCacheData( self, key ):
    if not self.__useCache:
      raise KeyError( "Cache use disabled" )
    return self.__dataCache[ key ]


  @property
  def jid( self ):
    return self.__jid

  @property
  def traceActions( self ):
    return self.__keepTrace

  def addActionToTrace( self, actionTuple ):
    self.__changes.append( actionTuple )

#
# Attributes
# 

  @TracedMethod
  def setStatus( self, majorStatus, minorStatus ):
    self.__cacheData( 'att.status', majorStatus )
    self.__cacheData( 'att.minorStatus', minorStatus )
    #TODO: Sync DB

  def getStatus( self ):
    try:
      return self.__getCacheData( 'att.status' )
    except KeyError:
      pass


  @TracedMethod
  def setMinorStatus( self, minorStatus ):
    self.__attrCache[ 'minorStatus' ] = minorStatus
    #TODO: Sync DB

#
# Params
#

  @TracedMethod
  def setParam( self, name, value ):
    self.__paramCache[ name ] = value
    #TODO: Sync DB



#
# Description stuff
#


  def isDesctiptionDirty( self ):
    return self.__descDirty

  def loadDescription( self, dataString ):
    """
    Auto discover format type based on [ .. ] of JDL
    """
    dataString = dataString.strip()
    if dataString[0] == "[" and dataString[-1] == "]":
      return self.loadDescriptionFromJDL( dataString )
    else:
      return self.loadDescriptionFromCFG( dataString )

  def loadDescriptionFromJDL( self, jdlString ):
    """
    Load job description from JDL format
    """
    result = loadJDLAsCFG( jdlString.strip() )
    if not result[ 'OK' ]:
      self.__description = CFG()
      return result
    self.__description = result[ 'Value' ][0]
    return S_OK()

  def loadDescriptionFromCFG( self, cfgString ):
    """
    Load job description from CFG format
    """
    try:
      self.__description.loadFromBuffer( cfgString )
    except Exception, e:
      return S_ERROR( "Can't load description from cfg: %s" % str( e ) )
    return S_OK()

  def dumpDescriptionAsCFG( self ):
    return str( self.__description )

  def dumpDescriptionAsJDL( self ):
    return dumpCFGAsJDL( self.__description )

  def __checkNumericalVarInDescription( self, varName, defaultVal, minVal, maxVal ):
    """
    Check a numerical var
    """
    initialVal = False
    if varName not in self.__description:
      varValue = gConfig.getValue( "/JobDescription/Default%s" % varName , defaultVal )
    else:
      varValue = self.__description[ varName ]
      initialVal = varValue
    try:
      varValue = long( varValue )
    except:
      return S_ERROR( "%s must be a number" % varName )
    minVal = gConfig.getValue( "/JobDescription/Min%s" % varName, minVal )
    maxVal = gConfig.getValue( "/JobDescription/Max%s" % varName, maxVal )
    varValue = max( minVal, min( varValue, maxVal ) )
    if initialVal != varValue:
      self.__description.setOption( varName, varValue )
    return S_OK( varValue )

  def __checkChoiceVarInDescription( self, varName, defaultVal, choices ):
    """
    Check a choice var
    """
    initialVal = False
    if varName not in self.__description:
      varValue = gConfig.getValue( "/JobDescription/Default%s" % varName , defaultVal )
    else:
      varValue = self.__description[ varName ]
      initialVal = varValue
    if varValue not in gConfig.getValue( "/JobDescription/Choices%s" % varName , choices ):
      return S_ERROR( "%s is not a valid value for %s" % ( varValue, varName ) )
    if initialVal != varValue:
      self.__description.setOption( varName, varValue )
    return S_OK( varValue )

  def __checkMultiChoiceInDescription( self, varName, choices ):
    """
    Check a multi choice var
    """
    initialVal = False
    if varName not in self.__description:
      return S_OK()
    else:
      varValue = self.__description[ varName ]
      initialVal = varValue
    choices = gConfig.getValue( "/JobDescription/Choices%s" % varName , choices )
    for v in List.fromChar( varValue ):
      if v not in choices:
        return S_ERROR( "%s is not a valid value for %s" % ( v, varName ) )
    if initialVal != varValue:
      self.__description.setOption( varName, varValue )
    return S_OK( varValue )

  def __checkMaxInputData( self, maxNumber ):
    """
    Check Maximum Number of Input Data files allowed
    """
    initialVal = False
    varName = "InputData"
    if varName not in self.__description:
      return S_OK()
    varValue = self.__description[ varName ]
    if len( List.fromChar( varValue ) ) > maxNumber:
      return S_ERROR( 'Number of Input Data Files (%s) greater than current limit: %s' % ( len( List.fromChar( varValue ) ) , maxNumber ) )
    return S_OK()

  def setDescriptionVarsFromDict( self, varDict ):
    for k in sorted( varDict ):
      self.setVar( k, varDict[ k ] )

  def checkDescription( self ):
    """
    Check that the description is OK
    """
    for k in [ 'OwnerName', 'OwnerDN', 'OwnerGroup', 'DIRACSetup' ]:
      if k not in self.__description:
        return S_ERROR( "Missing var %s in description" % k )
    #Check CPUTime
    result = self.__checkNumericalVarInDescription( "CPUTime", 86400, 0, 500000 )
    if not result[ 'OK' ]:
      return result
    result = self.__checkNumericalVarInDescription( "Priority", 1, 0, 10 )
    if not result[ 'OK' ]:
      return result
    allowedSubmitPools = []
    for option in [ "DefaultSubmitPools", "SubmitPools", "AllowedSubmitPools" ]:
      allowedSubmitPools = gConfig.getValue( "%s/%s" % ( getAgentSection( "WorkloadManagement/TaskQueueDirector" ), option ),
                                             allowedSubmitPools )
    result = self.__checkMultiChoiceInDescription( "SubmitPools", allowedSubmitPools )
    if not result[ 'OK' ]:
      return result
    result = self.__checkMultiChoiceInDescription( "PilotTypes", [ 'private' ] )
    if not result[ 'OK' ]:
      return result
    result = self.__checkMaxInputData( 500 )
    if not result[ 'OK' ]:
      return result
    result = self.__checkMultiChoiceInDescription( "JobType",
                                                   gConfig.getValue( "/Operations/JobDescription/AllowedJobTypes",
                                                                     [] ) )
    if not result[ 'OK' ]:
      #HACK to maintain backwards compatibility
      #If invalid set to "User"
      #HACKEXPIRATION 05/2009
      self.setVar( "JobType", "User" )
      #Uncomment after deletion of hack
      #return result
    return S_OK()

  def setDescriptionVar( self, varName, varValue ):
    """
    Set a var in job description
    """
    self.__descDirty = True
    levels = List.fromChar( varName, "/" )
    cfg = self.__description
    for l in levels[:-1]:
      if l not in cfg:
        cfg.createNewSection( l )
      cfg = cfg[ l ]
    cfg.setOption( levels[-1], varValue )

  def getDescriptionVar( self, varName, defaultValue = None ):
    """
     Get a variable from the job description
    """
    cfg = self.__description
    return cfg.getOption( varName, defaultValue )

  def getDescriptionVarList( self, section = "" ):
    """
    Get a list of variables in a section of the job description
    """
    cfg = self.__description.getRecursive( section )
    if not cfg or 'value' not in cfg:
      return []
    cfg = cfg[ 'value' ]
    return cfg.listOptions()

  def getDescriptionSectionList( self, section = "" ):
    """
    Get a list of sections in the job description
    """
    cfg = self.__description.getRecursive( section )
    if not cfg or 'value' not in cfg:
      return []
    cfg = cfg[ 'value' ]
    return cfg.listSections()
