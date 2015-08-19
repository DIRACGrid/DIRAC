'''
Created on May 5, 2015

@author: Corentin Berger
'''

import json
from json import JSONDecoder

from DIRAC.DataManagementSystem.Client.DataLogging.DLAction import DLAction
from DIRAC.DataManagementSystem.Client.DataLogging.DLFile import DLFile
from DIRAC.DataManagementSystem.Client.DataLogging.DLSequence import DLSequence
from DIRAC.DataManagementSystem.Client.DataLogging.DLMethodCall import DLMethodCall
from DIRAC.DataManagementSystem.Client.DataLogging.DLMethodName import DLMethodName
from DIRAC.DataManagementSystem.Client.DataLogging.DLCaller import DLCaller
from DIRAC.DataManagementSystem.Client.DataLogging.DLStorageElement import DLStorageElement
from DIRAC.DataManagementSystem.Client.DataLogging.DLUserName import DLUserName
from DIRAC.DataManagementSystem.Client.DataLogging.DLGroup import DLGroup
from DIRAC.DataManagementSystem.Client.DataLogging.DLHostName import DLHostName
from DIRAC import gLogger


class DLDecoder( json.JSONDecoder ):

    def __init__( self, *args, **kargs ):
        JSONDecoder.__init__( self, object_hook = self.dict_to_object,
                             *args, **kargs )

    def dict_to_object( self, d ):
      if '__type__' not in d:
          return d
      typeObj = d.pop( '__type__' )
      try:
        if typeObj == 'DLAction':
          obj = DLAction( d['file'], d['status'] , d['srcSE'], d['targetSE'], d['extra'], d['errorMessage'] )
        elif typeObj == 'DLSequence':
          obj = DLSequence( d['methodCalls'], d['caller'], d['sequenceID'], d['userName'], d['group'], d['hostName'], d['extra'] )
        elif typeObj == 'DLFile':
          obj = DLFile( d['name'] )
        elif typeObj == 'DLMethodCall':
          obj = DLMethodCall( d )
          obj.actions = d['actions']
          obj.children = d['children']
        elif typeObj == 'DLCaller':
          obj = DLCaller( d['name'] )
        elif typeObj == 'DLMethodName':
          obj = DLMethodName( d['name'] )
        elif typeObj == 'DLStorageElement':
          obj = DLStorageElement( d['name'] )
        elif typeObj == 'DLUserName':
          obj = DLUserName( d['name'] )
        elif typeObj == 'DLGroup':
          obj = DLGroup( d['name'] )
        elif typeObj == 'DLHostName':
          obj = DLHostName( d['name'] )
        else:
          obj = d
        return obj
      except Exception as e:
        gLogger.error( 'exception in DLDecoder %s for type %s' % ( e, typeObj ) )
        d['__type__'] = typeObj
        return d


