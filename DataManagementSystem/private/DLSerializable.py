'''
Created on Jul 3, 2015

@author: Corentin Berger
'''

import json
import datetime

from DIRAC import S_ERROR, S_OK

from DIRAC.DataManagementSystem.private.DLEncoder import DLEncoder

class DLSerializable( object ):
  _datetimeFormat = '%Y-%m-%d %H:%M:%S'
  def __init__( self ):
    pass

  def toJSON( self ):
    """ Returns the JSON description string """
    try:
      jsonStr = json.dumps( self, cls = DLEncoder )
      return S_OK( jsonStr )
    except Exception, e:
      return S_ERROR( str( e ) )


  def _getJSONData( self ):
    """ Returns the data that have to be serialized by JSON """
    jsonData = {}

    for attrName in self.attrNames :
      value = getattr( self, attrName, None )
      if isinstance( value, datetime.datetime ):
        # We convert date time to a string
        jsonData[attrName] = value.strftime( self._datetimeFormat )
      else:
        jsonData[attrName] = value

    jsonData['__type__'] = self.__class__.__name__

    return jsonData
