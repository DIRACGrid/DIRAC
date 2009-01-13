
import types
from DIRAC import S_OK, S_ERROR, gConfig
from DIRAC.ConfigurationSystem.Client.CFG import CFG
from DIRAC.Core.Utilities import List
from DIRAC.Core.Utilities.JDL import loadJDLAsCFG, dumpCFGAsJDL

class JobDescription:

  def __init__( self, jobID = 0 ):
    self.__jobID = jobID
    self.__description = CFG()

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
      return S_ERROR( "Can't load description from cfg: %s" % str(e) )
    return S_OK()

  def dumpDescriptionAsCFG( self ):
    return str( self.__description )

  def dumpDescriptionAsJDL( self ):
    return dumpCFGAsJDL( self.__description )

  def __checkNumericalVarInDescription( self, varName, defaultVal, minVal, maxVal ):
    """
    Check a numerical var
    """
    if varName not in self.__description:
      varValue = gConfig.getValue( "/TBD/Default%s" % varName , defaultVal )
    else:
      varValue = self.__description[ varName ]
    try:
      varValue = long( varValue )
    except:
      return S_ERROR( "%s must be a number" % varName )
    minVal = gConfig.getValue( "/TBD/Min%s" % varName, minVal )
    maxVal = gConfig.getValue( "/TBD/Max%s" % varName, maxVal )
    varValue = max( minVal, min( varValue, maxVal ) )
    self.__description.setOption( varName, varValue )
    return S_OK( varValue )

  def __checkChoiceVarInDescription( self, varName, defaultVal, choices ):
    """
    Check a choice var
    """
    if varName not in self.__description:
      varValue = gConfig.getValue( "/TBD/Default%s" % varName , defaultVal )
    else:
      varValue = self.__description[ varName ]
    if varValue not in gConfig.getValue( "/TBD/Choices%s" % varName , choices ):
      return S_ERROR( "%s is not a valid value for %s" % ( varValue, varName ) )
    self.__description.setOption( varName, varValue )
    return S_OK( varValue )

  def __checkMultiChoiceInDescription( self, varName, defaultValue, choices ):
    """
    Check a multi choice vair
    """
    if varName not in self.__description:
      defaultValue = gConfig.getValue( "/TBD/Default%s" % varName , defaultValue )
      if not defaultValue:
        return S_OK()
      varValue = defaultValue
    else:
      varValue = self.__description[ varName ]
    choices = gConfig.getValue( "/TBD/Choices%s" % varName , choices )
    for v in List.fromChar( varValue ):
      if v not in choices:
        return S_ERROR( "%s is not a valid value for %s" % ( v, varName ) )
    self.__description.setOption( varName, varValue )
    return S_OK( varValue )

  def setDescriptionVarsFromDict( self, varDict ):
    for k in sorted( varDict ):
      self.setDescriptionVar( k, varDict[ k ] )

  def checkDescription( self ):
    """
    Check that the description is OK
    """
    for k in [ 'OwnerName', 'OwnerDN', 'OwnerGroup', 'Setup' ]:
      if k not in self.__description:
        return S_ERROR( "Missing var %s in description" % k )
    #Check CPUTime
    result = self.__checkNumericalVarInDescription( "CPUTime", 86400, 0, 500000 )
    if not result[ 'OK' ]:
      return result
    result = self.__checkNumericalVarInDescription( "Priority", 5, 1, 10 )
    if not result[ 'OK' ]:
      return result
    result = self.__checkChoiceVarInDescription( "SubmissionPool", "default", [ 'default', 'sam' ] )
    if not result[ 'OK' ]:
      return result
    result = self.__checkMultiChoiceInDescription( "PilotTypes", "", [ 'generic', 'private' ] )
    if not result[ 'OK' ]:
      return result
    return S_OK()

  def setDescriptionVar( self, varName, varValue ):
    """
    Set a var in job description
    """
    levels = List.fromChar( varName, "/" )
    cfg = self.__description
    for l in levels[:-1]:
      if l not in cfg:
        cfg.createNewSection( l )
      cfg = cfg[ l ]
    cfg.setOption( levels[-1], varValue )