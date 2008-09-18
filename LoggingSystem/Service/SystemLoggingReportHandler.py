# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/LoggingSystem/Service/SystemLoggingReportHandler.py,v 1.8 2008/09/18 18:21:55 mseco Exp $
__RCSID__ = "$Id: SystemLoggingReportHandler.py,v 1.8 2008/09/18 18:21:55 mseco Exp $"
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
from DIRAC import S_OK, S_ERROR, gConfig, gLogger
from DIRAC.Core.Utilities import Time
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.LoggingSystem.private.Message import tupleToMessage
from DIRAC.LoggingSystem.DB.SystemLoggingDB import SystemLoggingDB

def initializeSystemLoggingReportHandler( serviceInfo ):

  global LogDB
  LogDB = SystemLoggingDB()
  return S_OK()


class SystemLoggingReportHandler( RequestHandler ):
  
  types_getMessages=[]
  
  def export_getMessages( self, selectionDict = {}, sortList = [], 
                          startItem = 0, maxItems = 0 ):
    """ Query the database for all the messages between two given dates.
        If no date is provided then the records returned are those generated
        during the last 24 hours.
    """
    from DIRAC.Core.Utilities import dateTime, day

    if selectionDict.has_key( 'beginDate' ):
      beginDate=selectionDict.pop( 'beginDate' )
    else:
      beginDate=None
    if selectionDict.has_key( 'endDate' ):
      endDate=selectionDict.pop( 'endDate' )
    else:
      endDate=None

    if not ( beginDate or endDate ):
      beginDate= dateTime() - 1 * day 
      
    result = LogDB._queryDB( newer = beginDate, older = endDate )

    if not result['OK']: return result
    
    retValue = { 'ParameterNames': [ 'MessageTime', 'LogLevel', 
                                   'FixedTextString', 'VariableText', 
                                   'SystemName', 'SubSystemName', 
                                   'OwnerDN', 'OwnerGroup',
                                   'ClientIPNumberString', 'SiteName' ], 
               'Records':  result['Value'], 
               'TotalRecords': len( result['Value'] ), 'Extras': {}}
    
    return S_OK( retValue )

  types_getCountMessages=[]

  def export_getCountMessages( self, selectionDict = {}, sortList = [], 
                           startItem = 0, maxItems = 0 ):
    """ Query the database for the number of messages that match 'conds' and
        were generated between initialDate and endDate. If no condition is
        provided it returns the total number of messages present in the
        database
    """
    if selectionDict.has_key( 'beginDate' ):
      beginDate=selectionDict.pop( 'beginDate' )
    else:
      beginDate=None
    if selectionDict.has_key( 'endDate' ):
      endDate=selectionDict.pop( 'endDate' )
    else:
      endDate=None

    if selectionDict:
      fieldList = selectionDict.keys()
      fieldList.append( 'MessageTime' )
    else:
      fieldList = [ 'MessageTime', 'LogLevel', 'FixedTextString',
                    'VariableText', 'SystemName', 'SubSystemName',
                    'OwnerDN', 'OwnerGroup', 'ClientIPNumberString',
                    'SiteName' ]

    result = LogDB._queryDB( showFieldList = fieldList, condDict = selectionDict, 
                                  older = endDate, newer = initialDate, 
                                  count = True )

    if not result['OK']: return result
    
    retValue={ 'ParameterNames': ['Number of Messages'], 'Records':  result['Value'][0][0],
               'TotalRecords': 1, 'Extras': {}}
    
    return S_OK( retValue )

  types_getGroupedMessages = []
  
  def export_getGroupedMessages( self, selectionDict = {}, sortList = [], 
                                 startItem = 0, maxItems = 0 ):
    """  This function reports the number of messages per fixed text
         string, system and subsystem that generated them using the 
         DIRAC convention for communications between services and 
         web pages
    """
    fieldList = [ 'MessageTime', 'LogLevel', 'SystemName', 'SubSystemName',
                  'FixedTextString', 'VariableText',  'OwnerDN', 'OwnerGroup',
                  'ClientIPNumberString', 'SiteName' ]

    if not ( selectionDict.has_key( 'LogLevel' ) and selectionDict['LogLevel'] ):
      selectionDict['LogLevel'] = [ 'ERROR', 'EXCEPT', 'FATAL' ]

    if selectionDict.has_key( 'beginDate' ):
      beginDate=selectionDict.pop( 'beginDate' )
    else:
      beginDate=None
    if selectionDict.has_key( 'endDate' ):
      endDate=selectionDict.pop( 'endDate' )
    else:
      endDate=None

    if selectionDict.has_key('groupField'):
      groupField=selectionDict.pop( 'groupField' )      
    else:
      groupField = 'FixedTextString'
      
    result = LogDB._queryDB( showFieldList = fieldList, condDict = selectionDict, 
                             older = endDate, newer = beginDate, 
                             count = True, groupColumn = groupField,
                             orderFields = sortList )

    if not result['OK']: return result

    if maxItems:
      records = result['Value'][ startItem:maxItems + startItem ]
    else:
      records = result['Value'][ startItem: ]

    if not sortList:
      unOrderedFields = [ ( s[-1], s ) for s in records ]
      unOrderedFields.sort()
      records = [ t[1] for t in unOrderedFields ]
      records.reverse()

    if 'count(*) as recordCount' in fieldList:
      fieldList.remove( 'count(*) as recordCount' )
    fieldList.append( 'Number of Errors' )

    retValue={ 'ParameterNames': fieldList, 'Records': records ,
           'TotalRecords': len( records ), 'Extras': {}}
  
    return S_OK( retValue )
  
  types_getSites = []

  def export_getSites( self, selectionDict = {}, sortList = [], 
                       startItem = 0, maxItems = 0 ):
    result = LogDB._queryDB( showFieldList = [ 'SiteName' ] )
    
    if not result['OK']: return result
    
    retValue={ 'ParameterNames': [ 'SiteName' ], 'Records':  result['Value'],
               'TotalRecords': len( result['Value'] ), 'Extras': {}}

    return S_OK( retValue )

  types_getSystems = []

  def export_getSystems( self , selectionDict = {}, sortList = [], 
                         startItem = 0, maxItems = 0 ):
    result = LogDB._queryDB( showFieldList = [ 'SystemName' ] )
    
    if not result['OK']: return result
    
    retValue={ 'ParameterNames': [ 'SystemName' ], 'Records':  result['Value'],
               'TotalRecords': len( result['Value'] ), 'Extras': {}}

    return S_OK( retValue )

  types_getSubSystems = []

  def export_getSubSystems( self, selectionDict = {}, sortList = [], 
                            startItem = 0, maxItems = 0 ):
    result = LogDB._queryDB( showFieldList = [ 'SubSystemName' ] )
    
    if not result['OK']: return result
    
    retValue={ 'ParameterNames': [ 'SubSystemName' ], 'Records':  result['Value'],
               'TotalRecords': len( result['Value'] ), 'Extras': {}}

    return S_OK( retValue )

  types_getGroups = []

  def export_getGroups( self, selectionDict = {}, sortList = [], 
                        startItem = 0, maxItems = 0 ):

    result = LogDB._queryDB( showFieldList = [ 'OwnerGroup' ] )
    
    if not result['OK']: return result
    
    retValue={ 'ParameterNames': [ 'OwnerGroup' ], 'Records':  result['Value'],
               'TotalRecords': len( result['Value'] ), 'Extras': {}}
      
    return S_OK( retValue )

  types_getFixedTextStrings = []

  def export_getFixedTextStrings( self, selectionDict = {}, sortList = [], 
                                  startItem = 0, maxItems = 0 ):
    result = LogDB._queryDB( showFieldList = [ 'FixedTextString' ] )
    
    if not result['OK']: return result
    
    retValue={ 'ParameterNames': [ 'FixedTextString' ], 'Records':  result['Value'],
               'TotalRecords': len( result['Value'] ), 'Extras': {}}
    
    return S_OK( retValue )
    