"""
SystemLoggingReportHandler allows a remote system to access the contest
of the SystemLoggingDB

    The following methods are available in the Service interface

    getTopErrors()
    getGroups()
    getSites()
    getSystems()
    getSubSystems()
    getFixedTextStrings()
    getCountMessages()
    getGroupedMessages()
    getMessages()
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from DIRAC import S_OK
from DIRAC.Core.Utilities import Time
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.FrameworkSystem.DB.SystemLoggingDB import SystemLoggingDB

__RCSID__ = "$Id$"


def initializeSystemLoggingReportHandler(serviceInfo):

  global LogDB
  LogDB = SystemLoggingDB()
  return S_OK()


class SystemLoggingReportHandler(RequestHandler):

  types_getMessages = []

  def __getMessages(self, selectionDict={}, sortList=[],
                    startItem=0, maxItems=0):
    """
    """
    from re import search

    if 'convertDates' in selectionDict:
      convertDatesToStrings = selectionDict['convertDates']
      del selectionDict['convertDates']
    else:
      convertDatesToStrings = True

    if convertDatesToStrings:
      dateField = "DATE_FORMAT(MessageTime, '%Y-%m-%d %H:%i:%s')"
    else:
      dateField = 'MessageTime'

    if 'count' in selectionDict:
      countMessages = selectionDict['count']
      del selectionDict['count']
    else:
      countMessages = True

    if 'beginDate' in selectionDict:
      beginDate = selectionDict['beginDate']
      del selectionDict['beginDate']
    else:
      beginDate = None
    if 'endDate' in selectionDict:
      endDate = selectionDict['endDate']
      del selectionDict['endDate']
    else:
      endDate = None

    if not (beginDate or endDate):
      beginDate = Time.date() - 1 * Time.day

    if 'groupField' in selectionDict:
      groupField = selectionDict['groupField']
      if groupField not in selectionDict:
        groupField = list(selectionDict)[0]
      del selectionDict['groupField']
    elif countMessages:
      if selectionDict:
        groupField = list(selectionDict)[0]
      elif sortList:
        groupField = sortList[0][0]
      else:
        groupField = 'FixedTextString'
    else:
      groupField = None

    if selectionDict:
      fieldList = list(selectionDict)
      fieldList.append(dateField)
      if not ('LogLevel' in selectionDict and
              selectionDict['LogLevel']):
        selectionDict['LogLevel'] = ['ERROR', 'EXCEPT', 'FATAL']
    else:
      fieldList = [dateField, 'LogLevel', 'FixedTextString',
                   'VariableText', 'SystemName', 'SubSystemName',
                   'OwnerDN', 'OwnerGroup', 'ClientIPNumberString',
                   'SiteName']
      selectionDict['LogLevel'] = ['ERROR', 'EXCEPT', 'FATAL']

    result = LogDB._queryDB(showFieldList=fieldList, condDict=selectionDict,
                            older=endDate, newer=beginDate,
                            count=countMessages, groupColumn=groupField,
                            orderFields=sortList)

    if not result['OK']:
      return result

    if maxItems:
      records = result['Value'][startItem:maxItems + startItem]
    else:
      records = result['Value'][startItem:]

    if not sortList:
      unOrderedFields = sorted([(s[-1], s) for s in records])
      records = [t[1] for t in unOrderedFields]
      records.reverse()

    if countMessages:
      if 'count(*) as recordCount' in fieldList:
        fieldList.remove('count(*) as recordCount')
      fieldList.append('Number of Errors')

    if convertDatesToStrings:
      for element in fieldList:
        if search('MessageTime', element):
          index = fieldList.index(element)
      fieldList[index] = 'MessageTime'

    retValue = {'ParameterNames': fieldList, 'Records': records,
                'TotalRecords': len(result['Value']), 'Extras': {}}

    return S_OK(retValue)

  def export_getMessages(self, selectionDict={}, sortList=[], startItem=0, maxItems=0):
    """ Query the database for all the messages between two given dates.
        If no date is provided then the records returned are those generated
        during the last 24 hours.
    """
    selectionDict['count'] = False
    selectionDict['groupField'] = None
    selectionDict['LogLevel'] = ['ERROR', 'EXCEPT', 'FATAL', 'ALWAYS']
    return self.__getMessages(selectionDict, sortList, startItem, maxItems)

  types_getCountMessages = []

  def export_getCountMessages(self, selectionDict={}, sortList=[], startItem=0, maxItems=0):
    """ Query the database for the number of messages that match 'conds' and
        were generated between initialDate and endDate. If no condition is
        provided it returns the total number of messages present in the
        database
    """
    selectionDict['count'] = True
    selectionDict['groupField'] = None

    return self.__getMessages(selectionDict, sortList, startItem, maxItems)

  types_getGroupedMessages = []

  def export_getGroupedMessages(self, selectionDict={}, sortList=[], startItem=0, maxItems=0):
    """  This function reports the number of messages per fixed text
         string, system and subsystem that generated them using the
         DIRAC convention for communications between services and
         web pages
    """
    selectionDict['count'] = True

    return self.__getMessages(selectionDict, sortList, startItem, maxItems)

  types_getSites = []

  def export_getSites(self, selectionDict={}, sortList=[], startItem=0, maxItems=0):
    result = LogDB._queryDB(showFieldList=['SiteName'])

    if not result['OK']:
      return result

    if maxItems:
      records = result['Value'][startItem:maxItems + startItem]
    else:
      records = result['Value'][startItem:]

    retValue = {'ParameterNames': ['SiteName'], 'Records': records,
                'TotalRecords': len(result['Value']), 'Extras': {}}

    return S_OK(retValue)

  types_getSystems = []

  def export_getSystems(self, selectionDict={}, sortList=[],
                        startItem=0, maxItems=0):
    result = LogDB._queryDB(showFieldList=['SystemName'])

    if not result['OK']:
      return result

    if maxItems:
      records = result['Value'][startItem:maxItems + startItem]
    else:
      records = result['Value'][startItem:]

    retValue = {'ParameterNames': ['SystemName'], 'Records': records,
                'TotalRecords': len(result['Value']), 'Extras': {}}

    return S_OK(retValue)

  types_getSubSystems = []

  def export_getSubSystems(self, selectionDict={}, sortList=[],
                           startItem=0, maxItems=0):
    result = LogDB._queryDB(showFieldList=['SubSystemName'])

    if not result['OK']:
      return result

    if maxItems:
      records = result['Value'][startItem:maxItems + startItem]
    else:
      records = result['Value'][startItem:]

    retValue = {'ParameterNames': ['SubSystemName'], 'Records': records,
                'TotalRecords': len(result['Value']), 'Extras': {}}

    return S_OK(retValue)

  types_getGroups = []

  def export_getGroups(self, selectionDict={}, sortList=[],
                       startItem=0, maxItems=0):

    result = LogDB._queryDB(showFieldList=['OwnerGroup'])

    if not result['OK']:
      return result

    if maxItems:
      records = result['Value'][startItem:maxItems + startItem]
    else:
      records = result['Value'][startItem:]

    retValue = {'ParameterNames': ['OwnerGroup'], 'Records': records,
                'TotalRecords': len(result['Value']), 'Extras': {}}

    return S_OK(retValue)

  types_getFixedTextStrings = []

  def export_getFixedTextStrings(self, selectionDict={}, sortList=[],
                                 startItem=0, maxItems=0):
    result = LogDB._queryDB(showFieldList=['FixedTextString'])

    if not result['OK']:
      return result

    if maxItems:
      records = result['Value'][startItem:maxItems + startItem]
    else:
      records = result['Value'][startItem:]

    retValue = {'ParameterNames': ['FixedTextString'], 'Records': records,
                'TotalRecords': len(result['Value']), 'Extras': {}}

    return S_OK(retValue)
