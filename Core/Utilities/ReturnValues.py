# $HeadURL$
"""
   DIRAC return dictionary
   Message values are converted to string
   keys are converted to string
"""

import types

def S_ERROR( messageString = '' ):
  """ return value on error confition
  :param string messageString: error description
  """
  return { 'OK' : False, 'Message' : str( messageString )  }

def S_OK( value = None ):
  return { 'OK' : True, 'Value' : value }

def isReturnStructure( unk ):
  if type( unk ) != types.DictType:
    return False
  if 'OK' not in unk:
    return False
  if unk[ 'OK' ]:
    if 'Value' not in unk:
      return False
  else:
    if 'Message' not in unk:
      return False
  return True

def returnSingleResult( dictRes ):
  """ Transform the S_OK{Successful/Failed} dictionary convention into
      an S_OK/S_ERROR return. To be used when a single returned entity
      is expected from a generally bulk call. 

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
      {'OK': True, 'Value': {'Successful': {}, 'Failed': {}}} -> {'Message': 'returnSingleResult: Failed and Successful dictionaries are empty', 'OK': False}
   """
  # if S_ERROR was returned, we return it as well
  if not dictRes['OK']:
    return dictRes
  # if there is a Failed, we return the first one in an S_ERROR
  if "Failed" in dictRes['Value'] and len( dictRes['Value']['Failed'] ):
    errorMessage = dictRes['Value']['Failed'].values()[0]
    return S_ERROR( errorMessage )
  # if there is a Successful, we return the first one in an S_OK
  elif "Successful" in dictRes['Value'] and len( dictRes['Value']['Successful'] ):
    return S_OK( dictRes['Value']['Successful'].values()[0] )
  else:
    return S_ERROR ( "returnSingleResult: Failed and Successful dictionaries are empty" )
                 

