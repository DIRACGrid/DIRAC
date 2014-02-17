""" Just an utilities collector
"""

import types
from  DIRAC import S_OK, S_ERROR

def checkArgumentFormat( path ):
  """ returns {'/this/is/an/lfn.1':False, '/this/is/an/lfn.2':False ...}
  """

  if type( path ) in types.StringTypes:
    return S_OK( {path:False} )
  elif type( path ) == types.ListType:
    return S_OK( dict( [( url, False ) for url in path if type( url ) in types.StringTypes] ) )
  elif type( path ) == types.DictType:
    return S_OK( path )
  else:
    return S_ERROR( "Utils.checkArgumentFormat: Supplied path is not of the correct format." )


def executeSingleFileOrDirWrapper( dictRes ):
  """ Transform the S_OK{Successful/Failed} dictionary convention into
      an S_OK/S_ERROR return.

      :param dictRes S_ERROR or S_OK( "Failed" : {}, "Successful" : {})
      :returns S_ERROR or S_OK(value)

      The following rules are applied:
      - if dictRes is an S_ERROR: returns it as is
      - we start by looking at the Failed directory
      - if there are several items in a dictionary, we return the first one
      - if both dictionaries are empty, we return S_ERROR
      - For an item in Failed, we return S_ERROR
      - Far an item in Successful we return S_OK

      Behavior examples (would be perfect unit test :-) ):

      {'Message': 'Kaput', 'OK': False} -> {'Message': 'Kaput', 'OK': False}
      {'OK': True, 'Value': {'Successful': {}, 'Failed': {'a': 1}}} -> {'Message': '1', 'OK': False}
      {'OK': True, 'Value': {'Successful': {'b': 2}, 'Failed': {}}} -> {'OK': True, 'Value': 2}
      {'OK': True, 'Value': {'Successful': {'b': 2}, 'Failed': {'a': 1}}} -> {'Message': '1', 'OK': False}
      {'OK': True, 'Value': {'Successful': {'b': 2}, 'Failed': {'a': 1, 'c': 3}}} -> {'Message': '1', 'OK': False}
      {'OK': True, 'Value': {'Successful': {'b': 2, 'd': 4}, 'Failed': {}}} -> {'OK': True, 'Value': 2}
      {'OK': True, 'Value': {'Successful': {}, 'Failed': {}}} -> {'Message': 'Utils.executSingleFileWrapper: Failed and Successful dictionaries are empty', 'OK': False}
   """
  # if S_ERROR was returned, we return it as well
  if not dictRes['OK']:
    return dictRes
  # if there is a failed, we return the first one in an S_ERROR
  if len( dictRes['Value']['Failed'] ):
    errorMessage = dictRes['Value']['Failed'].values()[0]
    return S_ERROR( errorMessage )
  # if there is a successful, we return the first one in an S_OK
  elif len( dictRes['Value']['Successful'] ):
    return S_OK( dictRes['Value']['Successful'].values()[0] )
  else:
    return S_ERROR ( "Utils.executSingleFileWrapper: Failed and Successful dictionaries are empty" )
                 
