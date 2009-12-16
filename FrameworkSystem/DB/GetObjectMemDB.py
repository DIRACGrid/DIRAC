# $Header: /local/reps/dirac/DIRAC3/DIRAC/LoggingSystem/DB/getObjectMemDB.py,v 1.29 2009/09/03 15:59:54 vfernand Exp $
__RCSID__ = "$Id: getObjectMemDB.py,v 1.29 2009/09/03 15:59:54 vfernand Exp $"
  
  
import types
from DIRAC                                     import gLogger
from DIRAC import S_OK, S_ERROR
  
class GetObjectMemDB:

  def __init__( self):
    """ Standard Constructor
    """
    self.__keyIdentDict={}

  def _insert(self, typeName, keyName, keyValue):
    #Cast to string just in case
    if type( keyValue ) != types.StringType:
      keyValue = str( keyValue )
    #No more than 64 chars for keys
    if len( keyValue ) > 64:
      keyValue = keyValue[:64]    
    #Look into the cache
    if typeName not in self.__keyIdentDict:
      self.__keyIdentDict[ typeName ] = {}
    typeCache = self.__keyIdentDict[ typeName ]
    if keyName not in typeCache:
      typeCache[ keyName ] = {}
    keyCache = typeCache[ keyName ]
    if keyValue in typeCache[ keyName ]:
      return S_OK( typeCache[ keyName ] )
    typeCache[keyName] = keyValue
  
  def _getValue(self, dictName , Key ):
    dictTemp = self.__keyIdentDict[dictName]
    return dictTemp[Key]
  
  def _getValueDict(self, dictName , key, keyPosition ):
    rangeVars=[]
    decTemp = self.__keyIdentDict[dictName]
    for k, v in decTemp.iteritems():
      if len(keyPosition)==1:
        if k[keyPosition[0]] in key[0]:
          rangeVars.append(decTemp[k])
      if len(keyPosition)==2:
        if k[keyPosition[0]] in key[0] and k[keyPosition[1]] in key[1]:
          rangeVars.append(decTemp[k])     
      if len(keyPosition)==3:
        if k[keyPosition[0]] in key[0] and k[keyPosition[1]] in key[1] and k[keyPosition[2]] in key[2]:
          rangeVars.append(decTemp[k])        
      if len(keyPosition)==4:
        if k[keyPosition[0]] in key[0]  and k[keyPosition[1]] in key[1] and k[keyPosition[2]] in key[2] and k[keyPosition[3]] in key[3]:
          rangeVars.append(decTemp[k])          
    return rangeVars
  
  def _getIntervalValue(self, dictName , Key, keyPosition, fieldsNumber):
    rangeVars=[]
    dictTemp = self.__keyIdentDict[dictName]
    for k, v in dictTemp.iteritems():
      if fieldsNumber == 4:
        if len(keyPosition)==1:
          if k[keyPosition[0]]==key[0]:
            rangeVars.append(dictTemp[k[0],k[1],k[2],k[4]])
        if len(keyPosition)==2:
          if k[keyPosition[0]]==key[0] and k[keyPosition[1]]==key[1]:
              rangeVars.append(dictTemp[k[0],k[1],k[2],k[4]])
        if len(keyPosition)==3:
          if k[keyPosition[0]]==key[0] and k[keyPosition[1]]==key[1] and k[keyPosition[2]]==key[2]:
              rangeVars.append(dictTemp[k[0],k[1],k[2],k[4]])
        if len(keyPosition)==4:
          if k[keyPosition[0]]==key[0] and k[keyPosition[1]]==key[1] and k[keyPosition[2]]==key[2] and k[keyPosition[3]]==key[3]:
              rangeVars.append(dictTemp[k[0],k[1],k[2],k[4]])
      if fieldsNumber == 3:
          if len(keyPosition)==1:
            if k[keyPosition[0]]==key[0]:
              rangeVars.append(dictTemp[k[0],k[1],k[2]])
          if len(keyPosition)==2:
            if k[keyPosition[0]]==key[0] and k[keyPosition[1]]==key[1]:
                rangeVars.append(dictTemp[k[0],k[1],k[2]])
          if len(keyPosition)==3:
            if k[keyPosition[0]]==key[0] and k[keyPosition[1]]==key[1] and k[keyPosition[2]]==key[2]:
                rangeVars.append(dictTemp[k[0],k[1],k[2]])
      if fieldsNumber == 2:
          if len(keyPosition)==1:
            if k[keyPosition[0]]==key[0]:
              rangeVars.append(dictTemp[k[0],k[1]])
          if len(keyPosition)==2:
            if k[keyPosition[0]]==key[0] and k[keyPosition[1]]==key[1]:
                rangeVars.append(dictTemp[k[0],k[1]])
    print rangeVars
    return rangeVars
  
  