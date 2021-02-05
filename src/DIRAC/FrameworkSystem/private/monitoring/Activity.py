from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

from string import Template
import six


class Activity:
  """
  This class is maintained in order to maintain a basic mapping of the sqlite3 database meaning
  what all tables and fields the database uses and it creates a mapping for them which is further
  used by the MonitoringCatalog class.
  """

  dbFields = ['activities.unit',
              'activities.type',
              'activities.description',
              'activities.filename',
              'activities.bucketLength',
              'sources.site',
              'sources.componentType',
              'sources.componentLocation',
              'sources.componentName'
              ]

  dbMapping = {
  }

  def __init__(self, dataList):
    """
    Init an activity
    """
    self.dataList = dataList
    self.groupList = []
    self.groupLabel = ""
    self.__initMapping()
    self.templateMap = {}
    self.scaleFactor = 1
    self.labelTemplate = ""
    for fieldName in self.dbFields:
      capsFieldName = fieldName.split(".")[1].upper()
      self.templateMap[capsFieldName] = self.__getField(fieldName)

  def __initMapping(self):
    """
    Init static maping
    """
    if not self.dbMapping:
      for index in range(len(self.dbFields)):
        self.dbMapping[self.dbFields[index]] = index

  def setBucketScaleFactor(self, scaleFactor):
    """
    Sets a bucket scale factor.
    """
    self.scaleFactor = scaleFactor
    self.__calculateUnit()

  def __calculateUnit(self):
    self.dataList = list(self.dataList)
    unit = self.dataList[self.dbMapping['activities.unit']].split("/")[0]
    if self.getType() in ("sum"):
      sF = int(self.getBucketLength() * self.scaleFactor) / 60
      if sF == 1:
        unit = "%s/min" % unit
      else:
        unit = "%s/%s mins" % (unit, sF)
    if self.getType() in ("rate"):
      unit = "%s/seconds" % unit
    self.dataList[self.dbMapping['activities.unit']] = unit
    self.templateMap['UNIT'] = unit

  def setGroup(self, group):
    """
    Set group to which this activity belongs
    """
    self.groupList = group
    self.groupLabel = "Grouped for"
    for fieldName in self.groupList:
      self.groupLabel += " %s," % fieldName
    self.groupLabel = self.groupLabel[:-1]

  def setLabel(self, labelTemplate):
    """
    Sets an activity label
    """
    self.labelTemplate = labelTemplate

  def __getField(self, name):
    """
    Gets a field value
    """
    return self.dataList[self.dbMapping[name]]

  def getUnit(self):
    """
    Gets an activity's unit.
    """
    return self.__getField('activities.unit')

  def getFile(self):
    """
    Gets an activity's file.
    """
    return self.__getField('activities.filename')

  def getType(self):
    """
    Gets an activity's type.
    """
    return self.__getField('activities.type')

  def getDescription(self):
    """
    Gets an activity's unit
    """
    return self.__getField('activities.description')

  def getBucketLength(self):
    """
    Gets an activity's bucket length.
    """
    return self.__getField('activities.bucketLength')

  def getSite(self):
    """
    Gets an source's site.
    """
    return self.__getField('sources.site')

  def getComponentType(self):
    """
    Gets an source's component type.
    """
    return self.__getField('sources.componentType')

  def getComponentName(self):
    """
    Gets an source's component name.
    """
    return self.__getField('sources.componentName')

  def getComponentLocation(self):
    """
    Gets an source's component location.
    """
    return self.__getField('sources.componentLocation')

  def getGroupLabel(self):
    """
    Gets a group's label.
    """
    return self.groupLabel

  def getLabel(self):
    if isinstance(self.labelTemplate, six.text_type):
      self.labelTemplate = self.labelTemplate.encode("utf-8")
    return Template(self.labelTemplate).safe_substitute(self.templateMap)

  def __str__(self):
    return "[%s][%s][%s]" % (self.getLabel(), self.getGroupLabel(), str(self.dataList))

  def __repr__(self):
    return self.__str__()

  def __lt__(self, act):
    label = self.getLabel()
    try:
      return label < act.getLabel()
    except BaseException:
      return label < act
