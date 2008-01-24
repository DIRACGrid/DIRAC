# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/AccountingSystem/Client/Types/BaseAccountingType.py,v 1.1 2008/01/24 11:03:33 acasajus Exp $
__RCSID__ = "$Id: BaseAccountingType.py,v 1.1 2008/01/24 11:03:33 acasajus Exp $"

import types
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities import Time

class BaseAccountingType:

  validDataValues = ( types.IntType, types.LongType )

  def __init__( self ):
    self.keyFieldsList = []
    self.valueFieldsList = []
    self.valuesList = []
    self.startTime = 0
    self.endTime = 0

  def checkType( self ):
    """
    Check that everything is defined
    """
    if len( self.keyFieldsList ) == 0:
      raise Exception( "keyFieldsList has to be filled prior to utilization" )
    if len( self.valueFieldsList ) == 0:
      raise Exception( "valueFieldsList has to be filled prior to utilization" )
    self.fieldsList = []
    self.fieldsList.extend( self.keyFieldsList )
    self.fieldsList.extend( self.valueFieldsList )
    if len( self.valuesList ) != len( self.fieldsList ):
      self.valuesList = [ None for i in range( len( self.fieldsList ) ) ]

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
    self.startTime = Time.dateTime()

  def setNowAsEndTime( self ):
    """
    Set current time as end time of the report
    """
    self.endTime = Time.dateTime()


  def setNowAsStartAndEndTime( self ):
    """
    Set current time as start and end time of the report
    """
    self.startTime = Time.dateTime()
    self.endTime = self.startTime

  def setValueByKey( self, key, value ):
    """
    Add value for key
    """
    if key not in self.fieldsList:
      return S_ERROR( "Key %s is not defined" % key )
    keyPos = self.fieldsList.index( key )
    self.valuesList[ keyPos ] = value
    return S_OK()

  def setValuesFromDict( self, dataDict ):
    """
    Set values from key-value dictionary
    """
    errKeys = []
    for key in dataDict:
      if not key in self.fieldsList:
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
      key = self.fieldsList[i]
      if self.valuesList[i] == None:
        errorList.append( "no value for %s" % key )
      if key in self.valueFieldsList and type( self.valuesList[i] ) not in self.validDataValues:
        errorList.append( "value for key %s is not int/long type" % key )
    if errorList:
      return S_ERROR( "Invalid values: %s" % ", ".join( errorList ) )
    if not self.startTime:
      return S_ERROR( "Start time has not been defined" )
    if type( self.startTime ) != Time._dateTimeType:
      return S_ERROR( "Start time is not a datetime object" )
    if not self.endTime:
      return S_ERROR( "End time has not been defined" )
    if type( self.endTime ) != Time._dateTimeType:
      return S_ERROR( "End time is not a datetime object" )
    return S_OK()

  def getDefinition( self ):
    """
    Get a tuple containing type definition
    """
    return (  self.__class__.__name__,
             self.keyFieldsList,
             self.valueFieldsList
           )

  def getValues( self ):
    """
    Get a tuple containing report values
    """
    return (  self.__class__.__name__,
             self.startTime,
             self.endTime,
             self.valuesList
           )