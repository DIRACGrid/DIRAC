########################################################################
# File: MetaQuery.py
# Author: A.T.
# Date: 24.02.2015
# $HeadID$
########################################################################

""" Utilities for managing metadata based queries
"""

__RCSID__ = "$Id$"

from DIRAC import S_OK, S_ERROR
import DIRAC.Core.Utilities.Time as Time

from types import ListType, DictType, StringTypes, IntType, LongType, FloatType
import json

FILE_STANDARD_METAKEYS = { 'SE': 'VARCHAR',
                           'CreationDate': 'DATETIME',
                           'ModificationDate': 'DATETIME',
                           'LastAccessDate': 'DATETIME',
                           'User': 'VARCHAR',
                           'Group': 'VARCHAR',
                           'Path': 'VARCHAR',
                           'Name': 'VARCHAR',
                           'FileName': 'VARCHAR',
                           'CheckSum': 'VARCHAR',
                           'GUID': 'VARCHAR',
                           'UID': 'INTEGER',
                           'GID': 'INTEGER',
                           'Size': 'INTEGER',
                           'Status': 'VARCHAR' }

FILES_TABLE_METAKEYS = { 'Name': 'FileName',
                         'FileName': 'FileName',
                         'Size': 'Size',
                         'User': 'UID',
                         'Group': 'GID',
                         'UID': 'UID',
                         'GID': 'GID',
                         'Status': 'Status' }

FILEINFO_TABLE_METAKEYS = { 'GUID': 'GUID',
                            'CheckSum': 'CheckSum',
                            'CreationDate': 'CreationDate',
                            'ModificationDate': 'ModificationDate',
                            'LastAccessDate': 'LastAccessDate' }


class MetaQuery( object ):

  def __init__( self, queryDict = None, typeDict = None ):

    self.__metaQueryDict = {}
    if queryDict is not None:
      self.__metaQueryDict = queryDict
    self.__metaTypeDict = {}
    if typeDict is not None:
      self.__metaTypeDict = typeDict

  def setMetaQuery( self, queryList, metaTypeDict = None ):
    """ Create the metadata query out of the command line arguments
    """
    if metaTypeDict is not None:
      self.__metaTypeDict = metaTypeDict
    metaDict = {}
    contMode = False
    value = ''
    for arg in queryList:
      if not contMode:
        operation = ''
        for op in ['>=','<=','>','<','!=','=']:
          if op in arg:
            operation = op
            break
        if not operation:
          return S_ERROR( 'Illegal query element %s' % arg )

        name,value = arg.split(operation)
        if not name in self.__metaTypeDict:
          return S_ERROR( "Metadata field %s not defined" % name )

        mtype = self.__metaTypeDict[name]
      else:
        value += ' ' + arg
        value = value.replace(contMode,'')
        contMode = False

      if value[0] in ['"', "'"] and value[-1] not in ['"', "'"]:
        contMode = value[0]
        continue

      if ',' in value:
        valueList = [ x.replace("'","").replace('"','') for x in value.split(',') ]
        mvalue = valueList
        if mtype[0:3].lower() == 'int':
          mvalue = [ int(x) for x in valueList if not x in ['Missing','Any'] ]
          mvalue += [ x for x in valueList if x in ['Missing','Any'] ]
        if mtype[0:5].lower() == 'float':
          mvalue = [ float(x) for x in valueList if not x in ['Missing','Any'] ]
          mvalue += [ x for x in valueList if x in ['Missing','Any'] ]
        if operation == "=":
          operation = 'in'
        if operation == "!=":
          operation = 'nin'
        mvalue = {operation:mvalue}
      else:
        mvalue = value.replace("'","").replace('"','')
        if not value in ['Missing','Any']:
          if mtype[0:3].lower() == 'int':
            mvalue = int(value)
          if mtype[0:5].lower() == 'float':
            mvalue = float(value)
        if operation != '=':
          mvalue = {operation:mvalue}

      if name in metaDict:
        if type(metaDict[name]) == DictType:
          if type(mvalue) == DictType:
            op,value = mvalue.items()[0]
            if op in metaDict[name]:
              if type(metaDict[name][op]) == ListType:
                if type(value) == ListType:
                  metaDict[name][op] = list( set( metaDict[name][op] + value) )
                else:
                  metaDict[name][op] = list( set( metaDict[name][op].append( value ) ) )
              else:
                if type(value) == ListType:
                  metaDict[name][op] = list( set( [metaDict[name][op]] + value) )
                else:
                  metaDict[name][op] = list( set( [metaDict[name][op],value]) )
            else:
              metaDict[name].update(mvalue)
          else:
            if type(mvalue) == ListType:
              metaDict[name].update({'in':mvalue})
            else:
              metaDict[name].update({'=':mvalue})
        elif type(metaDict[name]) == ListType:
          if type(mvalue) == DictType:
            metaDict[name] = {'in':metaDict[name]}
            metaDict[name].update(mvalue)
          elif type(mvalue) == ListType:
            metaDict[name] = list( set( (metaDict[name] + mvalue ) ) )
          else:
            metaDict[name] = list( set( metaDict[name].append( mvalue ) ) )
        else:
          if type(mvalue) == DictType:
            metaDict[name] = {'=':metaDict[name]}
            metaDict[name].update(mvalue)
          elif type(mvalue) == ListType:
            metaDict[name] = list( set( [metaDict[name]] + mvalue ) )
          else:
            metaDict[name] = list( set( [metaDict[name],mvalue] ) )
      else:
        metaDict[name] = mvalue

    self.__metaQueryDict = metaDict
    return S_OK( metaDict )

  def getMetaQuery( self ):

    return self.__metaQueryDict

  def getMetaQueryAsJson( self ):

    return json.dumps( self.__metaQueryDict )

  def applyQuery( self, userMetaDict ):
    """  Return a list of tuples with tables and conditions to locate files for a given user Metadata
    """
    def getOperands( value ):
      if type( value ) == ListType:
        return [ ('in', value) ]
      elif type( value ) == DictType:
        resultList = []
        for operation, operand in value.items():
          resultList.append( ( operation, operand ) )
        return resultList
      else:
        return [ ("=", value) ]

    def getTypedValue( value, mtype ):
      if mtype[0:3].lower() == 'int':
        return int( value )
      elif mtype[0:5].lower() == 'float':
        return float( value )
      elif mtype[0:4].lower() == 'date':
        return Time.fromString( value )
      else:
        return value

    for meta, value in self.__metaQueryDict.items():

      # Check if user dict contains all the requested meta data
      userValue = userMetaDict.get( meta, None )
      if userValue is None:
        if str( value ).lower() == 'missing':
          continue
        else:
          return S_OK( False )
      elif str( value ).lower() == 'any':
        continue

      mtype = self.__metaTypeDict[meta]
      try:
        userValue = getTypedValue( userValue, mtype )
      except ValueError:
        return S_ERROR( 'Illegal type for metadata %s: %s in user data' % ( meta, str( userValue ) ) )

      # Check operations
      for operation, operand in getOperands( value ):
        try:
          if type( operand ) == ListType:
            typedValue = [ getTypedValue( x, mtype ) for x in operand ]
          else:
            typedValue = getTypedValue( operand, mtype )
        except ValueError:
          return S_ERROR( 'Illegal type for metadata %s: %s in filter' % ( meta, str( operand ) ) )

        # Apply query operation
        if operation in ['>', '<', '>=', '<=']:
          if type( typedValue ) == ListType:
            return S_ERROR( 'Illegal query: list of values for comparison operation' )
          elif operation == '>' and typedValue >= userValue:
            return S_OK( False )
          elif operation == '<' and typedValue <= userValue:
            return S_OK( False )
          elif operation == '>=' and typedValue > userValue:
            return S_OK( False )
          elif operation == '<=' and typedValue < userValue:
            return S_OK( False )
        elif operation == 'in' or operation == "=":
          if type( typedValue ) == ListType and not userValue in typedValue:
            return S_OK( False )
          elif type( typedValue ) != ListType and userValue != typedValue:
            return S_OK( False )
        elif operation == 'nin' or operation == "!=":
          if type( typedValue ) == ListType and userValue in typedValue:
            return S_OK( False )
          elif type( typedValue ) != ListType and userValue == typedValue:
            return S_OK( False )

    return S_OK( True )
