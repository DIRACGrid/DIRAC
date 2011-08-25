"""
This module collects utility functions
"""

#############################################################################
# useful functions
#############################################################################

def where(c, f):
  return "Class " + str(c.__class__.__name__) + ", in Function " + (f.__name__)

def whoRaised(x):
  return "Exception: " + str(x.__class__.__name__) +", raised by " + str(x)

def assignOrRaise(value, set_, exc, obj, fun):
  """
  Check that a value is in a set or raise the corresponding exception
  If value is not None and is not in set, raise the corresponding
  exception, else return it
  """
  if value is not None and value not in set_:
    raise exc, where(obj, fun)
  else: return value

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
import copy, ast

id_fun = lambda x: x

# Import utils

def voimport(base_mod, voext):
  try:
    return  __import__(voext + base_mod, globals(), locals(), ['*'])
  except ImportError:
    return  __import__(base_mod, globals(), locals(), ['*'])

# (Duck) type checking

def isiterable(obj):
  import collections
  return isinstance(obj,collections.Iterable)

# Type conversion

def bool_of_string(s):
  """Convert a string into a boolean in a SANE manner."""
  if s.lower() == "true"    : return True
  elif s.lower() == "false" : return False
  else                      : raise ValueError, "Cannot convert %s to a boolean value" % s

def typedobj_of_string(s):
  if s == "":
    return s
  try:
    return ast.literal_eval(s)
  except (ValueError, SyntaxError): # Probably it's just a string
    return s

# String utils

# http://en.wikibooks.org/wiki/Algorithm_Implementation/Strings/Longest_common_substring#Python
def LongestCommonSubstring(S1, S2):
  M = [[0]*(1+len(S2)) for i in xrange(1+len(S1))]
  longest, x_longest = 0, 0
  for x in xrange(1,1+len(S1)):
    for y in xrange(1,1+len(S2)):
      if S1[x-1] == S2[y-1]:
        M[x][y] = M[x-1][y-1] + 1
        if M[x][y]>longest:
          longest = M[x][y]
          x_longest  = x
      else:
        M[x][y] = 0
  return S1[x_longest-longest: x_longest]

def CountCommonLetters(S1, S2):
  count = 0
  target = S2
  for l in S1:
    if l in S2:
      count = count+1
      target = target.strip(l)
  return count

# List utils

def list_(a):
  """Same as list() except if arg is a string, in this case, return
  [a]"""
  return (list(a) if type(a) != str else [a])

def list_split(l):
  return [i[0] for i in l], [i[1] for i in l]

def list_combine(l1, l2):
  return list(imap(lambda x,y: (x,y), l1, l2))

def list_flatten(l):
  res = []
  for e in l:
    for ee in e:
      res.append(ee)
  return res

# Dict utils

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

def dict_split(d):
  def dict_one_split(d):
    def dict_copy(d, k, v):
      copy_of_d = copy.deepcopy(d)
      copy_of_d[k] = v
      return copy_of_d

    for (k,v) in d.items():
      if type(v) == list:
        return [dict_copy(d,k,i) for i in v]

    return [d]

  def dict_split(ds):
    res = [dict_one_split(d) for d in ds]
    res = list_flatten(res)
    if res != ds: return dict_split(res)
    else:         return res

  return dict_split([d])

def dict_invert(dict_):
  res = {}
  for k in dict_:
    if not isiterable(dict_[k]):
      dict_[k] = [dict_[k]]
    for i in dict_[k]:
      try:
        res[i].append(k)
      except KeyError:
        res[i] = [k]

  return res

# XML utils

def xml_append(doc, tag, value=None, elt=None, **kw):
  new_elt = doc.createElement(tag)
  for k in kw:
    new_elt.setAttribute(k, kw[k])
  if value != None:
    textnode = doc.createTextNode(str(value))
    new_elt.appendChild(textnode)
  if elt != None:
    return elt.appendChild(new_elt)
  else:
    return doc.documentElement.appendChild(new_elt)

# SQL Utils
# These module generate ad-hoc SQL queries given a table and kwargs

import re

class SQLParam(str):
  pass

class SQLValues(object):
  null = SQLParam("NULL")
  now  = SQLParam("NOW()")

def sql_update_(table, kw):
  if kw == {}: return ""
  res = "UPDATE %s SET " % table
  for k in kw:
    res += ("%s=%s, " % (k, str(kw[k]))) if type(kw[k]) != str else ("%s='%s', " % (k, kw[k]))
  return res[:-2]

def sql_update(table, **kw):
  return sql_update_(table, kw)

def sql_insert_(table, kw):
  if kw == {}: return ""
  res = "INSERT INTO %s " % table
  res += "(" + reduce(lambda acc, k: acc + k + ", ", kw.keys(), "") + ") "
  res += "VALUES (" + reduce(lambda acc, k: acc + (str(k) if type(k) != str else "'" + k +"'") + ", ", kw.values(), "") + ")"
  return re.sub(r", \)", ")", res)

def sql_insert(table, **kw):
  return sql_instert_(table, kw)

def sql_insert_update_(table, keys, kw):
  if type(keys) != list:
    raise TypeError, "keys argument has to be a list"
  res1 = sql_insert_(table, kw)
  res2 = " ON DUPLICATE KEY UPDATE "
  for k in kw:
    if k not in keys:
      res2 += ("%s=%s, " % (k, str(kw[k]))) if type(kw[k]) != str else ("%s='%s', " % (k, kw[k]))
  return (res1 + res2)[:-2]

def sql_insert_update(table, keys, **kw):
  return sql_insert_update_(table, keys, kw)
