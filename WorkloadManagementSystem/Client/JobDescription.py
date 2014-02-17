"""
"""

__RCSID__ = "$Id$"

from DIRAC import S_OK, S_ERROR, gConfig
from DIRAC.ConfigurationSystem.Client.PathFinder import getAgentSection
from DIRAC.Core.Utilities.CFG import CFG
from DIRAC.Core.Utilities import List
from DIRAC.Core.Utilities.JDL import loadJDLAsCFG, dumpCFGAsJDL
from DIRAC.ConfigurationSystem.Client.Helpers.Operations            import Operations

class JobDescription:

  def __init__( self ):
    self.__description = CFG()
    self.__dirty = False

  def isDirty( self ):
    return self.__dirty

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
    initialVal = 0
    if varName not in self.__description:
      varValue = Operations().getValue( "JobDescription/Default%s" % varName , defaultVal )
    else:
      varValue = self.__description[ varName ]
      initialVal = varValue
    try:
      varValue = long( varValue )
    except:
      return S_ERROR( "%s must be a number" % varName )
    minVal = Operations().getValue( "JobDescription/Min%s" % varName, minVal )
    maxVal = Operations().getValue( "JobDescription/Max%s" % varName, maxVal )
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
      varValue = Operations().getValue( "JobDescription/Default%s" % varName , defaultVal )
    else:
      varValue = self.__description[ varName ]
      initialVal = varValue
    if varValue not in Operations().getValue( "JobDescription/Choices%s" % varName , choices ):
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
    choices = Operations().getValue( "JobDescription/Choices%s" % varName , choices )
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
      return S_ERROR( 'Number of Input Data Files (%s) greater than current limit: %s' % ( len( List.fromChar( varValue ) ), maxNumber ) )
    return S_OK()

  def setVarsFromDict( self, varDict ):
    for k in sorted( varDict ):
      self.setVar( k, varDict[ k ] )

  def checkDescription( self ):
    """
    Check that the description is OK
    """
    for k in [ 'OwnerName', 'OwnerDN', 'OwnerGroup', 'DIRACSetup' ]:
      if k not in self.__description:
        return S_ERROR( "Missing var %s in description" % k )
    # Check CPUTime
    result = self.__checkNumericalVarInDescription( "CPUTime", 86400, 0, 500000 )
    if not result[ 'OK' ]:
      return result
    result = self.__checkNumericalVarInDescription( "Priority", 1, 0, 10 )
    if not result[ 'OK' ]:
      return result
    allowedSubmitPools = []
    for option in [ "DefaultSubmitPools", "SubmitPools", "AllowedSubmitPools" ]:
      allowedSubmitPools += gConfig.getValue( "%s/%s" % ( getAgentSection( "WorkloadManagement/TaskQueueDirector" ),
                                                          option ),
                                             [] )
    result = self.__checkMultiChoiceInDescription( "SubmitPools", list( set( allowedSubmitPools ) ) )
    if not result[ 'OK' ]:
      return result
    result = self.__checkMultiChoiceInDescription( "PilotTypes", [ 'private' ] )
    if not result[ 'OK' ]:
      return result
    maxInputData = Operations().getValue( "JobDescription/MaxInputData", 500 )
    result = self.__checkMaxInputData( maxInputData )
    if not result[ 'OK' ]:
      return result
    transformationTypes = Operations().getValue( "Transformations/DataProcessing", [] )
    result = self.__checkMultiChoiceInDescription( "JobType", ['User', 'Test', 'Hospital'] + transformationTypes )
    return S_OK()

  def setVar( self, varName, varValue ):
    """
    Set a var in job description
    """
    self.__dirty = True
    levels = List.fromChar( varName, "/" )
    cfg = self.__description
    for l in levels[:-1]:
      if l not in cfg:
        cfg.createNewSection( l )
      cfg = cfg[ l ]
    cfg.setOption( levels[-1], varValue )

  def getVar( self, varName, defaultValue = None ):
    cfg = self.__description
    return cfg.getOption( varName, defaultValue )

  def getOptionList( self, section = "" ):
    cfg = self.__description.getRecursive( section )
    if not cfg:
      return []
    return cfg[ 'value' ].listOptions()

  def getSectionList( self, section = "" ):
    cfg = self.__description.getRecursive( section )
    if not cfg:
      return []
    return cfg[ 'value' ].listSections()
