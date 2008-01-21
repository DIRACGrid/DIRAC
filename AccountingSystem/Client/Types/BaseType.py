
import types
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities import Time

class BaseType:

  validDataValues = ( types.IntType, types.LongType )

  def __init__(self):
    self.descriptionKeysList = []
    self.keysList = []
    self.valuesList = []
    self.typeName = ""
    self.startTime = 0
    self.endTime = 0

  def checkType( self ):
    """
    Check that everything is defined
    """
    if len( self.descriptionKeysList ) == 0:
      raise Exception( "descriptionKeysList has to be filled prior to utilization" )
    if len( self.keysList ) == 0:
      raise Exception( "keysList has to be filled prior to utilization" )
    if len( self.valuesList ) != len( self.keysList ):
      self.valuesList = [ None for i in range( len( self.keysList ) ) ]
    if not self.typeName:
      raise Exception( "typeName has not been defined" )

  def setStartTime( self, startTime ):
    """
    Give a start time for the report
    """
    self.startTime = startTime

  def setEndTime( self, endTime ):
    """
    Give a end time for the report
    """
    self.endTime = endTime

  def setNowAsStartTime( self ):
    """
    Set current time as start time of the report
    """
    self.startTime = Time.datetime()

  def setNowAsEndTime( self ):
    """
    Set current time as end time of the report
    """
    self.endTime = Time.datetime()


  def setNowAsStartAndEndTime( self ):
    """
    Set current time as start and end time of the report
    """
    self.startTime = Time.datetime()
    self.endTime = self.startTime

  def setValueByKey( self, key, value ):
    """
    Add value for key
    """
    if key not in self.keysList:
      return S_ERROR( "Key %s is not defined" % key )
    keyPos = self.keysList.find( key )
    self.valuesList[ keyPos ] = value
    return S_OK()

  def setValues( self, valuesList ):
    """
    Set values to contents of list
    """
    if len( valuesList ) == len( self.valuesList ):
      self.valuesList = valuesList
      return S_OK()
    return S_ERROR( "Length mismatch between passing values list and list of keys" )

  def setValuesFromDict( self, dataDict ):
    """
    Set values from key-value dictionary
    """
    errKeys = []
    for key in dataDict:
      if not key in self.keysList:
        errKeys.append( key )
    if errKeys:
      return S_ERROR( "Key(s) %s are not valid" % ", ".join( errKeys ) )
    for key in dataDict:
      self.setValueByKey( key, dataDict[ key ] )
    return S_OK()

  def checkValues( self ):
    """
    Check that all values are defined and valid
    """
    errorList = []
    for i in range( len( self.valuesList ) ):
      key = self.keysList[i]
      if self.valuesList[i] == None:
        errorList.append( "no value for %s" % key )
      if key not in self.descriptionKeysList and type( self.valuesList[i] ) not in self.validDataValues:
        errorList.append( "value for key %s is not int/long type" % key )
    if errorList:
      return S_ERROR( "Invalid values: %s" % ", ".join( errorList ) )
    if not self.startTime:
      return S_ERROR( "Start time has not been defined" )
    if not self.endTime:
      return S_ERROR( "End time has not been defined" )
    return S_OK()

  def getDefinition( self ):
    """
    Get a tuple containing type definition
    """
    return ( self.typeName,
             self.descriptionKeysList,
             self.keysList
           )

  def getValues( self ):
    """
    Get a tuple containing report values
    """
    return ( self.typeName,
             self.startTime,
             self.endTime,
             self.valuesList
           )