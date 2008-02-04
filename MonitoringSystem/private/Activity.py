# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/MonitoringSystem/private/Activity.py,v 1.2 2008/02/04 11:49:55 acasajus Exp $
__RCSID__ = "$Id: Activity.py,v 1.2 2008/02/04 11:49:55 acasajus Exp $"

import types
from string import Template

class Activity:

  dbFields = [ 'activities.unit',
               'activities.type',
               'activities.description',
               'activities.filename',
               'sources.site',
               'sources.componentType',
               'sources.componentLocation',
               'sources.componentName'
              ]

  dbMapping = {
               }

  def __init__( self, dataList ):
    """
    Init an activity
    """
    self.dataList = dataList
    self.groupList = []
    self.label = ""
    self.groupLabel = ""
    self.__initMapping()
    self.templateMap = {}
    for fieldName in self.dbFields:
      capsFieldName = fieldName.split(".")[1].upper()
      self.templateMap[ capsFieldName ] = self.__getField( fieldName )

  def __initMapping(self):
    """
    Init static maping
    """
    if not self.dbMapping:
      for index in range( len( self.dbFields ) ):
        self.dbMapping[ self.dbFields[index] ] = index

  def setGroup( self, group ):
    """
    Set group to which this activity belongs
    """
    self.groupList = group
    self.groupLabel = "Grouped for"
    for fieldName in self.groupList:
      self.groupLabel += " %s," % fieldName
    self.groupLabel = self.groupLabel[:-1]

  def setLabel( self, labelTemplate ):
    """
    Set activity label
    """
    if type( labelTemplate ) == types.UnicodeType:
      labelTemplate = labelTemplate.encode( "utf-8" )
    self.label = Template( labelTemplate ).safe_substitute( self.templateMap )

  def __getField( self, name ):
    """
    Get field value
    """
    return self.dataList[ self.dbMapping[ name ] ]

  def getUnit(self):
    return self.__getField( 'activities.unit' )

  def getRRDUnit(self):
    if self.getType() == "rate":
      return "%s/sec" % self.getUnit()
    return "%s/min" % self.getUnit()

  def getFile(self):
    return self.__getField( 'activities.filename' )

  def getType(self):
    return self.__getField( 'activities.type' )

  def getDescription(self):
    return self.__getField( 'activities.description' )

  def getSite(self):
    return self.__getField( 'sources.site' )

  def getComponentType(self):
    return self.__getField( 'sources.componentType' )

  def getComponentName(self):
    return self.__getField( 'sources.componentName' )

  def getComponentLocation(self):
    return self.__getField( 'sources.componentLocation' )

  def getGroupLabel(self):
    return self.groupLabel

  def getLabel(self):
    return self.label

  def __str__( self ):
    return "[%s][%s][%s]" % ( self.getLabel(), self.getGroupLabel(), str( self.dataList ) )

  def __repr__( self ):
    return self.__str__()