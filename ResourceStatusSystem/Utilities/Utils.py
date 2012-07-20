# $HeadURL $
''' Utils

  Module that collects utility functions.

'''

import collections

from DIRAC                import gConfig, S_OK
from DIRAC.Core.Utilities import List

from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations

__RCSID__ = '$Id: $'

#############################################################################
# useful functions
#############################################################################

def getTypedList( stringValue ):
  '''
  Returns a typed list from a csv
  '''
  return [ typedobj_of_string(e) for e in List.fromChar( stringValue ) ]

#def where(c, f):
#  return "Class " + str(c.__class__.__name__) + ", in Function " + (f.__name__)

def convertTime(t, inTo = None):

  if inTo is None or inTo in ('second', 'seconds'):

    sec = 0

    try:
      tms = t.milliseconds
      sec = sec + tms/1000
    except AttributeError:
      pass
    try:
      ts = t.seconds
      sec = sec + ts
    except AttributeError:
      pass
    try:
      tm = t.minutes
      sec = sec + tm * 60
    except AttributeError:
      pass
    try:
      th = t.hours
      sec = sec + th * 3600
    except AttributeError:
      pass
    try:
      td = t.days
      sec = sec + td * 86400
    except AttributeError:
      pass
    try:
      tw = t.weeks
      sec = sec + tw * 604800
    except AttributeError:
      pass

    return sec

  elif inTo in ('hour', 'hours'):

    hour = 0

    try:
      tms = t.milliseconds
      hour = hour + tms/36000
    except AttributeError:
      pass
    try:
      ts = t.seconds
      hour = hour + ts/3600
    except AttributeError:
      pass
    try:
      tm = t.minutes
      hour = hour + tm/60
    except AttributeError:
      pass
    try:
      th = t.hours
      hour = hour + th
    except AttributeError:
      pass
    try:
      td = t.days
      hour = hour + td * 24
    except AttributeError:
      pass
    try:
      tw = t.weeks
      hour = hour + tw * 168
    except AttributeError:
      pass

    return hour

############################
# vibernar utils functions #
############################

from itertools import imap
import copy, ast, socket

id_fun = lambda x: x

# Import utils

def voimport( base_mod ):
  
  for ext in gConfig.getValue( 'DIRAC/Extensions', [] ):
  
    try:
      return  __import__( ext + base_mod, globals(), locals(), ['*'] )
    except ImportError:
      continue
  # If not found in extensions, import it in DIRAC base.
  return  __import__( base_mod, globals(), locals(), ['*'] )

# socket utils

def canonicalURL(url):
  try:
    canonical = socket.gethostbyname_ex(url)[0]
    return canonical
  except:
    return url

# RPC utils

#class RPCError(Exception):
#  pass
#
#def unpack(dirac_value):
#  if type(dirac_value) != dict:
#    raise ValueError, "Not a DIRAC value."
#  if 'OK' not in dirac_value.keys():
#    raise ValueError, "Not a DIRAC value."
#  try:
#    return dirac_value['Value']
#  except KeyError:
#    raise RPCError, dirac_value['Message']

#def protect2(f, *args, **kw):
#  """Wrapper protect"""
#  try:
#    ret = f(*args, **kw)
#    if type(ret) == dict and ret['OK'] == False:
#      print "function " + f.f.__name__ + " called with " + str( args )
#      print "%s\n" % ret['Message']
#    return ret
#  except Exception as e:
#    print "function " + str(f) + " called with " + str(args)
#    raise e

# (Duck) type checking

def isiterable(obj):
  return isinstance(obj,collections.Iterable)

# Type conversion

def getCSTree( csPath = '' ):
  '''
    Gives the configuration rooted at path in a Python dict. The
    result is a Python dictionnary that reflects the structure of the
    config file.
  '''

  opHelper = Operations()

  def getCSTreeAsDict( path ):
    
    csTreeDict = {}
    
    opts = opHelper.getOptionsDict( path )
    if opts[ 'OK' ]:
      
      opts = opts[ 'Value' ]
    
      for optKey, optValue in opts.items():
        if optValue.find( ',' ) > -1:
          optValue = List.fromChar( optValue )
        else:
          optValue = [ optValue ]
        csTreeDict[ optKey ] = optValue    
    
    secs = opHelper.getSections( path )
    if secs[ 'OK' ]:
      
      secs = secs[ 'Value' ]
            
      for sec in secs:
      
        secTree = getCSTreeAsDict( '%s/%s' % ( path, sec ) )
        if not secTree[ 'OK' ]:
          return secTree
      
        csTreeDict[ sec ] = secTree[ 'Value' ]  
    
    return S_OK( csTreeDict )
    
  return getCSTreeAsDict( csPath )  

def typedobj_of_string(s):
  if s == '_none_':
    return []
  if s == '': #isinstance( s, str ):
    return [ s ]
  try:
    return ast.literal_eval(s)
  except (ValueError, SyntaxError): # Probably it's just a string
    return s

# List utils

#def list_(a):
#  """Same as list() except if arg is a string, in this case, return
#  [a]"""
#  return (list(a) if type(a) != str else [a])

def list_flatten(l):
  try:
    return [ee for e in l for ee in e]
  except TypeError:
    return l

def list_sanitize(l):
  """Remove doublons and results that evaluate to false"""
  try:
    return list(set([i for i in l if i]))
  except TypeError:
    return [i for i in l if i]

def set_sanitize(l):
  """Remove doublons and results that evaluate to false"""
  try:
    return set([i for i in l if i])
  except TypeError:
    return [i for i in l if i]

# Dict utils

def configMatch( candidateParams, configParams ):
  '''
    For a given configuration, the candidate will be rejected if:
#    - it is missing at least one of the params in the config
    - if a param of the candidate does not match the config params  
    - if a candidate param is None, is considered as wildcard
  '''

  for key in candidateParams:
    
    if not key in configParams:
      # The candidateParams is missing one of the parameters required
      # return False
      continue
    
    if candidateParams[ key ] is None:
      # None is assumed to be a wildcard (*)
      continue 
    
    cParameter = candidateParams[ key ]
    if not isinstance( cParameter, list ):
      cParameter = [ cParameter ]
    
    elif not set( cParameter ).intersection( set( configParams[ key ] ) ):
      return False
    
  return True  
#  for key in configParams:
#    
#    if not key in candidateParams:
#      # The candidateParams is missing one of the parameters required
#      # return False
#      continue
#    
#    if candidateParams[ key ] is None:
#      # None is assumed to be a wildcard (*)
#      continue 
#    
#    cParameter = candidateParams[ key ]
#    if not isinstance( cParameter, list ):
#      cParameter = [ cParameter ]
#    
#    elif not set( cParameter ).intersection( set( configParams[ key ] ) ):
#      return False
#    
#  return True  

def dictMatch(dict1, dict2):
  """Checks if fields of dict1 are in fields of dict2. Returns True if
  it is the case and False otherwise."""
  if type(dict1) != type(dict2) != dict:
    raise TypeError, "dictMatch expect dicts for both arguments"

  numMatch = False

  for k in dict1:
    try:
      if dict1[k] == None:
        continue
      if dict1[k] not in dict2[k]:
        return False
      else:
        numMatch = True
    except KeyError:
      pass

  return numMatch

# XML utils

#def xml_append(doc, tag, value_=None, elt_=None, **kw):
#  new_elt = doc.createElement(tag)
#  for k in kw:
#    new_elt.setAttribute(k, str(kw[k]))
#  if value_ != None:
#    textnode = doc.createTextNode(str(value_))
#    new_elt.appendChild(textnode)
#  if elt_ != None:
#    return elt_.appendChild(new_elt)
#  else:
#    return doc.documentElement.appendChild(new_elt)

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF