# $Header: /local/reps/dirac/DIRAC3/DIRAC/LoggingSystem/DB/SystemLoggingDB.py,v 1.29 2009/09/03 15:59:54 vfernand Exp $
__RCSID__ = "$Id$"
""" SystemLoggingDB class is a front-end to the Message Logging Database.
    The following methods are provided

    insertMessageIntoSystemLoggingDB()
    getMessagesByDate()
    getMessagesByFixedText()
    getMessages()
"""    

import re, os, sys, string
import time
import threading
from types import ListType, StringTypes, NoneType

from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd
from types                                     import *
from DIRAC                                     import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Base.DB import DB
from DIRAC.Core.Utilities import Time, dateTime, hour, date, week, day, fromString, toString
from DIRAC.FrameworkSystem.private.logging.LogLevels import LogLevels
from DIRAC.FrameworkSystem.DB.GetObjectMemDB import GetObjectMemDB

DEBUG = 1

if DEBUG:
  debugFile = open( "SystemLoggingDB.debug.log", "w" )

getObject = GetObjectMemDB() 

###########################################################
class SystemLoggingDB(DB):

  def __init__( self, maxQueueSize=10 ):
    """ Standard Constructor
    """
    DB.__init__( self, 'SystemLoggingDB', 'Logging/SystemLoggingDB',
                 maxQueueSize)
    self._insertDataIntoMemDB()

  def _query( self, cmd, conn=False ):
    start = time.time()
    ret = DB._query( self, cmd, conn )
    if DEBUG:
      print >> debugFile, time.time() - start, cmd.replace('\n','')
      debugFile.flush()
    return ret

  def _update( self, cmd, conn=False ):
    start = time.time()
    ret = DB._update( self, cmd, conn )
    if DEBUG:
      print >> debugFile, time.time() - start, cmd.replace('\n','')
      debugFile.flush()
    return ret

  def __buildCondition(self, condDict, older=None, newer=None ):
    """ build SQL condition statement from provided condDict
        and other extra conditions
    """
    condition = ''
    conjonction = ''

    if condDict:
      for attrName, attrValue in condDict.items():
        preCondition = ''
        conjonction = ''
        if type( attrValue ) in StringTypes:
          attrValue = [ attrValue ]
        elif  type( attrValue) is NoneType:
          continue
        elif not type( attrValue ) is ListType:
          errorString = 'The values of conditions should be strings or lists'
          errorDesc = 'The type provided was: %s' % type ( attrValue )
          gLogger.warn( errorString, errorDesc )
          return S_ERROR( '%s: %s' % ( errorString, errorDesc ) )
        
        for attrVal in attrValue:
          preCondition = "%s%s %s='%s'" % ( preCondition, conjonction,
                                            str( attrName ), str( attrVal ) )
          conjonction = " OR"

        if condition:
          condition += " AND" 
        condition += ' (%s )' % preCondition

      conjonction = " AND"
      
    if older:
      condition = "%s%s MessageTime<'%s'" % ( condition,  conjonction,
                                              str( older ) )
      conjonction = " AND"

    if newer:
      condition = "%s%s MessageTime>'%s'" % ( condition, conjonction,
                                              str( newer ) )

    if condition:
      gLogger.debug( '__buildcondition:',
                     'condition string = "%s"' % condition )
      condition = " WHERE%s" % condition
      
    return S_OK(condition)
  
  def __buildConditionForMessageRepository(self, condDict, older=None, 
                                           newer=None ):
    """ build SQL condition statement from provided condDict
        and other extra conditions
    """
    condition = ''
    conjonction = ''

    mRepAttr = 'MessageTime' , 'VariableText' , 'LogLevel' 
    if condDict:
      for attrName, attrValue in condDict.items():
        preCondition = ''
        conjonction = ''
        if type( attrValue ) in StringTypes:
          attrValue = [ attrValue ]
        elif  type( attrValue) is NoneType:
          continue
        elif not type( attrValue ) is ListType:
          errorString = 'The values of conditions should be strings or lists'
          errorDesc = 'The type provided was: %s' % type ( attrValue )
          gLogger.warn( errorString, errorDesc )
          return S_ERROR( '%s: %s' % ( errorString, errorDesc ) )
        
        if attrName in mRepAttr:
          for attrVal in attrValue:
            preCondition = "%s%s %s='%s'" % ( preCondition, conjonction,
                                              str( attrName ), str( attrVal ) )
            conjonction = " OR"
  
          if condition:
            condition += " AND" 
          condition += ' (%s )' % preCondition

      conjonction = " AND"
      
    if older:
      condition = "%s%s MessageTime<'%s'" % ( condition,  conjonction,
                                              str( older ) )
      conjonction = " AND"

    if newer:
      condition = "%s%s MessageTime>'%s'" % ( condition, conjonction,
                                              str( newer ) )

    if condition:
      gLogger.debug( '__buildConditionForMessageRepository:',
                     'condition string = "%s"' % condition )
      condition = " AND%s" % condition
      
    return S_OK(condition)
  
  def _buildConditionTest(self, condDict, olderDate = None, newerDate = None ):
    """ a wrapper to the private function __buildCondition so test programs
        can access it
    """
    return self.__buildCondition( condDict, older = olderDate,
                                  newer = newerDate )

  def __uniq( self, array ):
    """http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/52560
    """

    arrayLength = len( array )
    if arrayLength == 0:
        return []

    sortDictionary = {}

    for key in array:
      sortDictionary[ key ] = 1
    return sortDictionary.keys()

      
  def __buildTableList( self, showFieldList ):
    """ build the SQL list of tables needed for the query
        from the list of variables provided
    """
    import re
    idPattern = re.compile( r'ID' )
       
    tableDict = { 'MessageTime':'MessageRepository',
                  'VariableText':'MessageRepository',
                  'LogLevel':'MessageRepository',
                  'FixedTextString':'FixedTextMessages',
                  'ReviewedMessage':'FixedTextMessages',
                  'SystemName':'Systems', 'SubSystemName':'SubSystems',
                  'OwnerDN':'UserDNs', 'OwnerGroup':'UserDNs',
                  'ClientIPNumberString':'ClientIPs',
                  'ClientFQDN':'ClientIPs', 'SiteName':'Sites'}
    tableDictKeys = tableDict.keys()
    tableList = []

    conjunction = ' NATURAL JOIN '
    gLogger.debug( '__buildTableList:', 'showFieldList = %s' % showFieldList )
    if len(showFieldList):
      for field in showFieldList:
        if not idPattern.search( field ) and ( field in tableDictKeys ):
          tableList.append( tableDict[field] )
      tableList = self.__uniq(tableList)
      tableString = ''
      if tableList.count('Sites') and tableList.count('MessageRepository') and not \
        tableList.count('ClientIPs'): 
        tableList.append('ClientIPs')     
      if tableList.count('MessageRepository') and tableList.count('SubSystems') \
        and not tableList.count('FixedTextMessages') \
        and not tableList.count('Systems'):
          tableList.append('FixedTextMessages')       
          tableList.append('Systems')
      if tableList.count('MessageRepository') and tableList.count('Systems') \
        and not tableList.count('FixedTextMessages'):
        tableList.append('FixedTextMessages')                  
      if tableList.count('FixedTextMessages') and tableList.count('SubSystems') \
        and not tableList.count('Systems'):
        tableList.append('Systems')      
      if tableList.count('MessageRepository') or ( tableList.count('FixedTextMessages') \
        + tableList.count('ClientIPs') + tableList.count('UserDNs') > 1 ) :
        tableString = 'MessageRepository'
      try:
        tableList.pop( tableList.index( 'MessageRepository') )
        tableString = 'MessageRepository'
      except:
        pass      
      if tableString and len(tableList):
        tableString = '%s%s' % ( tableString, conjunction ) 
      tableString = '%s%s' % ( tableString, 
                                 conjunction.join( tableList ) )

    else:
      tableString = conjunction.join( self.__uniq( tableDict.values() ) )
    
    gLogger.debug( '__buildTableList:', 'tableString = "%s"' % tableString )
    
    return tableString
  
  def _queryDB( self, showFieldList=None, condDict=None, older=None,
                 newer=None, count=False, groupColumn=None, orderFields=None ):
    """ This function composes the SQL query from the conditions provided and
        the desired columns and queries the SystemLoggingDB.
        If no list is provided the default is to use all the meaningful
        variables of the DB
    """
    grouping=''
    ordering=''
    result = self.__buildCondition( condDict, older, newer )
    if not result['OK']: return result
    condition = result['Value']
    
    if not showFieldList:
      showFieldList = ['MessageTime', 'LogLevel', 'FixedTextString',
                     'VariableText', 'SystemName', 
                     'SubSystemName', 'OwnerDN', 'OwnerGroup',
                     'ClientIPNumberString','SiteName']
    elif type( showFieldList ) in StringTypes:
      showFieldList = [ showFieldList ]
    elif not type( showFieldList ) is ListType:
      errorString = 'The showFieldList variable should be a string or a list of strings'
      errorDesc = 'The type provided was: %s' % type ( attrValue )
      gLogger.warn( errorString, errorDesc )
      return S_ERROR( '%s: %s' % ( errorString, errorDesc ) )
       
    tableList = self.__buildTableList( showFieldList )
              
    if groupColumn:
      grouping='GROUP BY %s' % groupColumn

    if count: 
      if groupColumn:
        showFieldList.append( 'count(*) as recordCount' )
      else:
        showFieldList = [ 'count(*) as recordCount' ]

    sortingFields = []
    if orderFields:
      for field in orderFields:
        if type( field ) == ListType:
          sortingFields.append( ' '.join(field) )
        else:
          sortingFields.append( field ) 
      ordering='ORDER BY %s' %  ', '.join( sortingFields )
    
    cmd = 'SELECT %s FROM %s %s %s %s' % ( ','.join(showFieldList ),
                                    tableList, condition, grouping, ordering )
    if condDict!=None:
      ids=self.__extractValues( condDict )
      returned = self.__getConditionsIDs( ids, tableList, grouping, ordering, 
                                         showFieldList, condDict, older, newer )
    if condDict==None:
      gLogger.debug("_Query:",cmd)
      return self._query(cmd) 
    returned_finish={}
    returned_finish['OK']=True
    returned_finish['Value'] = ()    
    i=0    
    k=0
    while (i<len(returned)):
      if (returned[i]['OK']==False) and (returned[i]['Value']!="") and ( k==0 ):
        returned_finish['OK']=False
        returned_finish['Value'] = returned[i]['Value']
        k=k+1
      i=i+1
    i=0
    dev=[]
    if (k == 0):
      while (i<len(returned)):
        if (returned[i]['OK']==True) and (returned[i]['Value']!=()):
          j=0
          while j<len(returned[i]['Value']):
            dev.append(returned[i]['Value'][j])
            j=j+1
        i=i+1
    v=tuple(dev)
    #New Returned Query
    returned_finish['Value']=v
    #Query old -> self._query(cmd)    
    return returned_finish
  
  def __getConditionsIDs(self, cmd2 = None, tableList = None, grouping = None, ordering = None, 
                        showFieldList=None, condDict=None, older=None, newer=None ):
    i=0
    j=0
    k=0
    inclusion = []
    conditionClientIP = 0
    conditionFixed = 0
    conditionUser = 0
    key= condDict.keys()
    if 'ClientIPNumberString' not in key:
      condDict['ClientIPNumberString']=None
    if 'ClientFQDN' not in key:
      condDict['ClientFQDN']=None
    if 'SiteName' not in key:
      condDict['SiteName']=None
    if 'FixedTextString' not in key:
      condDict['FixedTextString']=None
    if 'SystemName' not in key:
      condDict['SystemName']=None
    if 'SubSystemName' not in key:
      condDict['SubSystemName']=None
    if 'ReviewedMessage' not in key:
      condDict['ReviewedMessage']=None
    if 'OwnerDN' not in key:
      condDict['OwnerDN']=None
    if 'OwnerGroup' not in key:
      condDict['OwnerGroup']=None
    if 'MessageTime' not in key:
      condDict['MessageTime']=None
    if 'VariableText' not in key:
      condDict['VariableText']=None
    if 'LogLevel' not in key:
      condDict['LogLevel']=None
                        
    if (len(cmd2['ClientIPsSites']) == 0 and ( condDict['ClientIPNumberString'] == None and 
        condDict['ClientFQDN'] == None and 
        condDict['SiteName'] == None )):
      conditionClientIP = 1
    if (len(cmd2['FixedTextMessagesSystemsSubSystems']) == 0 and ( condDict['FixedTextString'] == None and 
        condDict['SystemName'] == None and
        condDict['SubSystemName'] == None and 
        condDict['ReviewedMessage'] == None)):
      conditionFixed = 1
    if (len(cmd2['UserDNs']) == 0 and (condDict['OwnerDN'] == None and condDict['OwnerGroup'] == None)):
      conditionUser = 1

    result = self.__buildConditionForMessageRepository(condDict, older, newer ) 
    if not result['OK']: return result
    condition = result['Value']    
    
    if len(cmd2['FixedTextMessagesSystemsSubSystems'])!=0 and conditionClientIP==1 and conditionUser==1:
     while i<len(cmd2['FixedTextMessagesSystemsSubSystems']):
      cmd = 'SELECT %s FROM %s WHERE FixedTextID = %s %s %s %s' %  ( ','.join(showFieldList ), tableList, 
        cmd2['FixedTextMessagesSystemsSubSystems'][i] ,  condition , 
        grouping, ordering )
      returned = self._query(cmd)
      inclusion.append(returned)
      i=i+1
    if conditionFixed==1 and len(cmd2['ClientIPsSites'])!=0 and conditionUser==1:
     while j<len(cmd2['ClientIPsSites']):
      cmd = 'SELECT %s FROM %s WHERE ClientIPNumberID = %s %s %s %s' % ( ','.join(showFieldList ), tableList, 
        cmd2['ClientIPsSites'][j] ,  condition , 
        grouping, ordering )
      returned = self._query(cmd)
      inclusion.append(returned)      
      j=j+1
    if len(cmd2['FixedTextMessagesSystemsSubSystems'])!=0 and len(cmd2['ClientIPsSites'])!=0 and conditionUser==1:
      while i<len(cmd2['FixedTextMessagesSystemsSubSystems']):
        while j<len(cmd2['ClientIPsSites']):
         cmd = 'SELECT %s FROM %s WHERE FixedTextID = %s AND ClientIPsSites= %s %s %s %s' % ( ','.join(showFieldList ), tableList, 
           cmd2['FixedTextMessagesSystemsSubSystems'][i], 
           cmd2['ClientIPsSites'][j] ,  condition , 
           grouping, ordering )
         returned = self._query(cmd)
         inclusion.append(returned)         
         j=j+1
        j=0
        i=i+1
    if conditionFixed==1 and conditionClientIP==1 and len(cmd2['UserDNs'])!=0:
      while k<len(cmd2['UserDNs']):
        cmd = 'SELECT %s FROM %s WHERE UserDNID = %s %s %s %s' % ( ','.join(showFieldList ), tableList, 
          cmd2['UserDNs'][k] , condition , 
          grouping, ordering )
        returned = self._query(cmd)
        inclusion.append(returned) 
        k=k+1   
    if len(cmd2['FixedTextMessagesSystemsSubSystems'])!=0 and conditionClientIP==1 and len(cmd2['UserDNs'])!=0:
      while i<len(cmd2['FixedTextMessagesSystemsSubSystems']):
        while j<len(cmd2['UserDNs']):    
          cmd = 'SELECT %s FROM %s WHERE FixedTextID = %s AND UserDNID= %s %s %s %s' % ( ','.join(showFieldList ), tableList, 
            cmd2['FixedTextMessagesSystemsSubSystems'][i], 
            cmd2['UserDNs'][j] ,  condition , 
            grouping, ordering )
          returned = self._query(cmd)
          inclusion.append(returned)           
          j=j+1
        j=0
        i=i+1              
    if conditionFixed==1 and len(cmd2['ClientIPsSites'])!=0 and len(cmd2['UserDNs'])!=0:
      while i<len(cmd2['ClientIPsSites']):
        while j<len(cmd2['UserDNs']):    
         cmd = 'SELECT %s FROM %s WHERE ClientIPNumberID = %s AND UserDNID= %s %s %s %s' % ( ','.join(showFieldList ), tableList, 
           cmd2['ClientIPsSites'][i], 
           cmd2['UserDNs'][j] ,  condition , 
           grouping, ordering )
         returned = self._query(cmd)
         inclusion.append(returned) 
         j=j+1
        j=0
        i=i+1    
    if len(cmd2['FixedTextMessagesSystemsSubSystems'])!=0 and len(cmd2['ClientIPsSites'])!=0 and len(cmd2['UserDNs'])!=0:
      while i<len(cmd2['FixedTextMessagesSystemsSubSystems']):
        while j<len(cmd2['ClientIPsSites']):
          while k<len(cmd2['UserDNs']):  
           cmd = 'SELECT %s FROM %s WHERE ClientIPNumberID = %s AND UserDNID= %s AND FixedTextID = %s %s %s %s' % ( ','.join(showFieldList ), tableList, 
                 cmd2['ClientIPsSites'][j], 
                 cmd2['UserDNs'][k], 
                 cmd2['FixedTextMessagesSystemsSubSystems'][i] , condition ,  grouping, ordering )
           returned = self._query(cmd)
           inclusion.append(returned) 
           k=k+1
          k=0
          j=j+1
        j=0
        i=i+1   
    return inclusion
        
  def __extractValues(self, condDict = None, dictName = None):
    
    tableDictPosition = { 'FixedTextString': 0 ,
              'ReviewedMessage': 1 ,
              'SystemName': 2 , 
              'SubSystemName': 3 ,
              'OwnerDN': 0 , 
              'OwnerGroup': 1 ,
              'ClientIPNumberString': 0 ,
              'ClientFQDN': 1 , 
              'SiteName': 2 }
    relationTablesDict = { 'FixedTextString': 'FixedTextMessagesSystemsSubSystems' ,
              'ReviewedMessage': 'FixedTextMessagesSystemsSubSystems' ,
              'SystemName': 'FixedTextMessagesSystemsSubSystems' , 
              'SubSystemName': 'FixedTextMessagesSystemsSubSystems' ,
              'OwnerDN': 'UserDNs' , 
              'OwnerGroup': 'UserDNs' ,
              'ClientIPNumberString': 'ClientIPsSites' ,
              'ClientFQDN': 'ClientIPsSites' , 
              'SiteName': 'ClientIPsSites' }
    numberColumsDict = { 'FixedTextMessagesSystemsSubSystems' : 4 ,
              'UserDNs': 2 , 
              'ClientIPsSites': 3 }
    
    tableDictKeys = relationTablesDict.keys()
    temporalStore = {}
    temporalStore['FixedTextMessagesSystemsSubSystems']={}
    temporalStore['UserDNs']={}
    temporalStore['ClientIPsSites']={}    
    dictionaries = ['FixedTextMessagesSystemsSubSystems','UserDNs','ClientIPsSites']

    tmpValues=[]
    tmpPosition=[]
    condKeysContent = []
    returnedValue={}
    tmpReturned=[]
    if len(condDict):
      for field in condDict:
        if ((field in tableDictKeys) and (condDict[field] !=None)):  
          tmp=temporalStore[relationTablesDict[field]]
          tmp[tableDictPosition[field]]=condDict[field]
      for k,v in temporalStore['FixedTextMessagesSystemsSubSystems'].iteritems():
            tmpPosition.append(k)
            tmpValues.append(v)
      tmpReturned = getObject._getValueDict('FixedTextMessagesSystemsSubSystems',tmpValues,tmpPosition)
      gLogger.debug("Values of FixedTextMessagesSystemsSubSystems Dictionary :", tmpReturned)
      returnedValue['FixedTextMessagesSystemsSubSystems']=tmpReturned 
      tmpValues=[]
      tmpPosition=[]     
      for k,v in temporalStore['UserDNs'].iteritems():
            tmpPosition.append(k)
            tmpValues.append(v)
      tmpReturned = getObject._getValueDict('UserDNs',tmpValues,tmpPosition)
      gLogger.debug("Values of UserDNs Dictionary :", tmpReturned)
      returnedValue['UserDNs']=tmpReturned  
      tmpValues=[]
      tmpPosition=[]
      for k,v in temporalStore['ClientIPsSites'].iteritems():
            tmpPosition.append(k)
            tmpValues.append(v)
      tmpReturned = getObject._getValueDict('ClientIPsSites',tmpValues,tmpPosition)
      gLogger.debug("Values of ClientIPsSites Dictionary :", tmpReturned)
      returnedValue['ClientIPsSites']=tmpReturned        
      return returnedValue
    return returnedValue
  
  
  
  def __DBCommit( self, tableName, outFields, inFields, inValues ):
    """  This is an auxiliary function to insert values on a
         satellite Table if they do not exist and returns
         the unique KEY associated to the given set of values
    """
    
    cmd = "SHOW COLUMNS FROM " + tableName + " WHERE Field in ( '" \
          +  "', '".join( inFields ) + "' )"
    result = self._query( cmd )
    gLogger.verbose(result)
    if ( not result['OK'] ) or result['Value'] == ():
      gLogger.debug(result['Message'])
      return S_ERROR( 'Could not get description of the %s table' % tableName )
    for description in result['Value']:
      if re.search( 'varchar', description[1] ):
        indexInteger = inFields.index( description[0] )
        valueLength = len( inValues[ indexInteger ] )
        fieldLength = int( re.search( r'varchar\((\d*)\)', 
                           description[1] ).groups()[0] )
        if fieldLength < valueLength:
          inValues[ indexInteger ] = inValues[ indexInteger ][ :fieldLength ]

    result = self._getFields( tableName, outFields, inFields, inValues )
    if not result['OK']:
      return S_ERROR('Unable to query the database')
    elif result['Value']==():
      result = self._insert( tableName, inFields, inValues )
      if not result['OK']:
        return S_ERROR( 'Could not insert the data into %s table' % tableName )

      result = self._getFields( tableName, outFields, inFields, inValues )
      if not result['OK']:
        return S_ERROR( 'Unable to query the database' )
      if result['Value']==():
        # The inserted value is larger than the field size and can not be matched back
        for i in range(len(inFields)):
          gLogger.error( 'Could not insert the data into %s table' % tableName, '%s = %s' % ( inFields[i], inValues[i] ) )
        return S_ERROR( 'Could not insert the data into %s table' % tableName )
      
    return S_OK( int( result['Value'][0][0] ) )
      
  def _insertMessageIntoSystemLoggingDB( self, message, site, nodeFQDN, 
                                         userDN, userGroup, remoteAddress ):
    """ This function inserts the Log message into the DB
    """
    messageDate = Time.toString( message.getTime() )
    messageDate = messageDate[:messageDate.find('.')]
    messageName = message.getName()
    messageSubSystemName = message.getSubSystemName()
    
    fieldsList = [ 'MessageTime', 'VariableText' ]
    messageList = [ messageDate, message.getVariableMessage() ]

    inValues = [ userDN, userGroup ]
    inFields = [ 'OwnerDN', 'OwnerGroup' ]
    outFields = [ 'UserDNID' ]
    result = self.__DBCommit( 'UserDNs', outFields, inFields, inValues )
    if not result['OK']:
      return result
    userDNskey = result['Value']
    messageList.append( result['Value'] )
    fieldsList.extend( outFields )

    tuplaUserDNs=(inValues[0],inValues[1])
    self.__addDateInCacheBack(('UserDNs') , tuplaUserDNs , userDNskey  )

    #Insert pair of tables ClientIPs and Sites 
    cmd = "SELECT ClientIPNumberID FROM ClientIPs cip, Sites si WHERE " \
          +  " cip.SiteID = si.SiteID AND " \
          +  " SiteName = '" + site + "' AND " \
          +  " ClientIPNumberString = '" +  remoteAddress + "' AND " \
          +  " ClientFQDN = '" + nodeFQDN + "' " 
    if not result['OK']:
      return result
    if result['Value']==():
      if not site:
        site = 'Unknown'
      inFields = [ 'SiteName']
      inValues = [ site ]
      outFields = [ 'SiteID' ]
      result = self.__DBCommit( 'Sites', outFields, inFields, inValues )
      if not result['OK']:
        return result
      siteIDKey = result['Value']
      
      inValuesSites = inValues[0]
      
      inFields = [ 'ClientIPNumberString' , 'ClientFQDN', 'SiteID' ]
      inValues = [ remoteAddress, nodeFQDN, siteIDKey ]
      outFields = [ 'ClientIPNumberID' ]
      result = self.__DBCommit( 'ClientIPs', outFields, inFields, inValues )
      if not result['OK']:
        return result
      clienIPkey = result['Value']  
      messageList.append(result['Value'])
      fieldsList.extend( outFields )
  
      tuplaSiteClientIPNumber = ( inValues[0],inValues[1],inValuesSites )
      self.__addDateInCacheBack(('ClientIpsSites') , 
                                tuplaSiteClientIPNumber , clienIPkey )
      self.__addDateInCacheBack(('SitesClientIps') , 
                                tuplaSiteClientIPNumber , siteIDKey )
    if result['Value']!=():
      outFields = [ 'ClientIPNumberID' ]
      messageList.append( result['Value'] )
      fieldsList.extend( outFields )
      
    messageList.append( message.getLevel() )
    fieldsList.append( 'LogLevel' )

    #Insert pair of tables ClientIPs and Sites 
    #ReviewedMessage have as default value 0
    cmd = "SELECT FixedTextID FROM FixedTextMessages fix, Systems sys, SubSystems sub WHERE " \
          +  " fix.SystemID = sys.SystemID AND " \
          +  " sys.SubSystemID = sub.SubSystemID AND " \
          +  " FixedTextString = '" + message.getFixedMessage() + "' AND " \
          +  " ReviewedMessage = 0 AND " \
          +  " SystemName = '" + messageName + "' AND " \
          +  " SubSystemName = '" + messageSubSystemName + "' " 
    if not result['OK']:
      return result
    if result['Value']==():      
      if not messageSubSystemName:
        messageSubSystemName = 'Unknown'
      inFields = [ 'SubSystemName' ]
      inValues = [ messageSubSystemName ]
      outFields = [ 'SubSystemID' ]
      result = self.__DBCommit( 'SubSystems', outFields, inFields, inValues )
      if not result['OK']:
        return result
      subSystemsKey = result['Value']
      
      inValuesSubSystemName = inValues[0]
      
      if not messageName:
        messageName = 'Unknown'
      inFields = [ 'SystemName', 'SubSystemID' ]
      inValues = [ messageName, subSystemsKey ]
      outFields = [ 'SystemID'  ]
      result = self.__DBCommit( 'Systems', outFields, inFields, inValues)
      if not result['OK']:
        return result
      SystemIDKey = result['Value']  
  
      tuplaSystemNameSubSystemName = ( inValues[0], inValuesSubSystemName)    
      self.__addDateInCacheBack(('SystemsSubSystems') , 
                                tuplaSystemNameSubSystemName , SystemIDKey )
      self.__addDateInCacheBack(('SubSystemsSystems') , 
                                tuplaSystemNameSubSystemName , subSystemsKey )
      
      inFields = [ 'FixedTextString' , 'ReviewedMessage' , 'SystemID' ]
      inValues = [ message.getFixedMessage(), SystemIDKey ]
      outFields = [ 'FixedTextID' ]
      result = self.__DBCommit( 'FixedTextMessages', outFields, inFields,
                                inValues)
      if not result['OK']:
        return result
      FixedTextIDKey = result['Value']  
      
      tuplaFixedSystemsSubSystems = ( inValues[0] , inValues[1], 
                                      tuplaSystemNameSubSystemName )
      self.__addDateInCacheBack(('FixedTextMessages','Systems','SubSystems') , 
                                tuplaFixedSystemsSubSystems , FixedTextIDKey )
          
      messageList.append( result['Value'] )
      fieldsList.extend( outFields )
    if result['Value']!=():
      outFields = [ 'FixedTextID' ]
      messageList.append( result['Value'] )
      fieldsList.extend( outFields )
            
    return self._insert( 'MessageRepository', fieldsList, messageList )

  def _insertDataIntoMemDB( self ):
    """ This function inserts the Log message into the Mem
    """    
    retVal = self._query("SELECT SystemID, SystemName, SubSystemName, sub.SubSystemID " \
                          +  " FROM Systems sys, SubSystems sub " \
                          +  " WHERE sys.SubSystemID = sub.SubSystemID ORDER BY SystemID" )
    if not retVal[ 'OK' ]:
      raise Exception( retVal[ 'Message' ] )
    
    for typesEntry in retVal[ 'Value' ]:
      tuplaSystemNameSubSystemName = (typesEntry[1],typesEntry[2])
      self.__addDateInCacheBack(('SystemsSubSystems') , 
                                tuplaSystemNameSubSystemName , typesEntry[0] )
      self.__addDateInCacheBack(('SubSystemsSystems') , 
                                tuplaSystemNameSubSystemName , typesEntry[3] )
          
    retVal = self._query("SELECT FixedTextID, FixedTextString, ReviewedMessage, " \
                         +  " SystemName, SubSystemName FROM Systems sys, " \
                         +  " SubSystems sub, FixedTextMessages fix " \
                         +  " WHERE " \
                         +  " sys.SubSystemID = sub.SubSystemID AND " \
                         +  " fix.SystemID = sys.SystemID " \
                         +  " ORDER BY FixedTextID" )
    if not retVal[ 'OK' ]:
      raise Exception( retVal[ 'Message' ] )
    for typesEntry in retVal[ 'Value' ]:
      tuplaFixSysSubSys = (typesEntry[1],typesEntry[2],typesEntry[3],typesEntry[4])
      self.__addDateInCacheBack(('FixedTextMessages','Systems','SubSystems') ,  
                                tuplaFixSysSubSys , typesEntry[0] )
      
       
    retVal = self._query("SELECT ClientIPNumberID, ClientIPNumberString,  " \
                         + "ClientFQDN, SiteName, si.SiteID " \
                         + "FROM ClientIPs cip, Sites si  " \
                         + "WHERE cip.SiteID = si.SiteID" )
    if not retVal[ 'OK' ]:
      raise Exception( retVal[ 'Message' ] )
    for typesEntry in retVal[ 'Value' ]:
      tuplaClientIPsSites = (typesEntry[1],typesEntry[2],typesEntry[3])      
      self.__addDateInCacheBack(('ClientIPsSites') , 
                                  tuplaClientIPsSites , typesEntry[0] )      
      self.__addDateInCacheBack(('SitesClientIPs') , 
                                  tuplaClientIPsSites , typesEntry[4] )
    
    retVal = self._query("SELECT UserDNID, OwnerDN, OwnerGroup FROM UserDNs" )
    if not retVal[ 'OK' ]:
      raise Exception( retVal[ 'Message' ] )
    for typesEntry in retVal[ 'Value' ]:
      tuplaUserDNs = (typesEntry[1],typesEntry[2])      
      self.__addDateInCacheBack(('UserDNs') , tuplaUserDNs , typesEntry[0] )
    #getObject._getKey("FixedTextMessagesSystemsSubSystems")  
    #getObject._getKey("SystemsSubSystems")  
    #getObject._getKey("SubSystemsSystems")  
    #getObject._getKey("ClientIPsSites")  
    #getObject._getKey("SitesClientIPs")   
    #getObject._getKey("UserDNs")
    print "------------"
    hola= ('user82', 'lhcb')
    print getObject._getValue("UserDNs",hola)   
    print "------------"
    
  def _insertDataIntoAgentTable(self, agentName, data):
    """Insert the persistent data needed by the agents running on top of
       the SystemLoggingDB.
    """
    result = self._escapeString( data )
    if not result['OK']:
      return result
    escapedData=result['Value']  

    outFields = ['AgentID']
    inFields = [ 'AgentName' ]
    inValues = [ agentName ]
    
    result = self._getFields('AgentPersistentData', outFields, inFields, inValues)
    if not result ['OK']:
      return result
    elif result['Value'] == ():
      inFields = [ 'AgentName', 'AgentData' ]
      inValues = [ agentName, escapedData]
      result=self._insert( 'AgentPersistentData', inFields, inValues )
      if not result['OK']:
        return result
    cmd = "UPDATE LOW_PRIORITY AgentPersistentData SET AgentData='%s' WHERE AgentID='%s'" % \
          ( escapedData, result['Value'][0][0] )
    return self._update( cmd )

  def _getDataFromAgentTable( self, agentName ):
    outFields = [ 'AgentData' ]
    inFields = [ 'AgentName' ]
    inValues = [ agentName ]

    return self._getFields('AgentPersistentData', outFields, inFields, inValues)


  def __getIdForKeyValue( self, tableA, tableB, conn = False ):
    """
      Finds id number of two tables
    """
    tableDictKeys = { 'Systems' : 'SystemID' ,
                      'SubSystems' : 'SubSystemID',
                      'FixedTextMessages' : 'FixedTextId',
                      'MessageRepository' : 'MessageID',
                      'UserDNs' : 'UserDNID',
                      'ClientIPs' : 'ClientIPNumberID',
                      'Sites' : 'SiteID' }
    idTbA = tableDictKeys[tableA]
    idTbB = tableDictKeys[tableB]
    retVal = self._query("SELECT A.%s, B.%s FROM %s A,%s B " \
                          + "WHERE A.%s = B.%s ORDER BY A.%s " % idTbA, idTbB, tableA, tableB, idTbA, idTB, idTbA )
    if not retVal[ 'OK' ]:
      return retVal
    if len( retVal[ 'Value' ] ) > 0:
      return S_OK( retVal[ 'Value' ][0][0] )
    return S_ERROR( "Key id %s for value %s does not exist although it shoud" % ( keyName, keyValue ) )
    
  def __addDateInCacheBack(self, showTableList, showFieldList, showIdList):    
    """
      Create a new dictionary or insert new date
    """
    if (( 'FixedTextMessages' in showTableList ) and 
        ( 'Systems' in showTableList ) and 
        ( 'SubSystems' in showTableList )):
          getObject._insert('FixedTextMessagesSystemsSubSystems',
                            showFieldList,showIdList)
    if ( 'SystemsSubSystems' in showTableList ):
          getObject._insert('SystemsSubSystems',showFieldList,showIdList)
    if ( 'SubSystemsSystems' in showTableList ):
          getObject._insert('SubSystemsSystems',showFieldList,showIdList)      
    if ( 'ClientIPsSites' in showTableList ):
          getObject._insert('ClientIPsSites',showFieldList,showIdList)
    if ( 'SitesClientIPs' in showTableList ):
          getObject._insert('SitesClientIPs',showFieldList,showIdList)
    if ( 'UserDNs' in showTableList ):
          getObject._insert('UserDNs',showFieldList,showIdList)
                 
