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
  except ValueError: # Probably it's just a string
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

  try:
    for k in dict1:
      if dict1[k] not in dict2[k]:
        return False
    return True
  except KeyError:
    # some keys are in dict1 and not in dict2: We don't care (in this
    # case).
    pass

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

# CLI stuff

class GetForm(object):
  """This class asks the user to fill a form inside a CLI. It checks
  the type of entered values and keep on asking them until the form
  has the correct type."""

  prompt = "> "
  form   = None

  def __init__(self, form):
    """form is a dict in the form label:<type or set of values>"""
    self.form = form

  def run(self):
    res = {}
    for i in self.form:
      res[i] = self.getval(i, self.form[i])
    return res

  def getval(self, label, restr, acceptFalse=False):
    """Restriction can be based on a type, or on a list of acceptable
    values. If valueTrue, then the value provided"""
    value = None

    if type(restr) == type:
      # Checks that the provided value is of type restr.
      if not acceptFalse:
        while type(value) != restr or not value:
          print "Enter value for %s: %s" % (label, str(restr))
          value = raw_input(self.prompt)
      else:
        while type(value) != restr:
          print "Enter value for %s: %s" % (label, str(restr))
          value = raw_input(self.prompt)

      return value

    else:
      # Checks that the provided value(s) are in the iterable

      if not acceptFalse:
        while not value:
          print "Enter value for %s: " % label
          value = self.pickvals(restr)
      else:
        print "Enter value for %s: " % label
        value = self.pickvals(restr)

      return value

  def pickvals(self, iterable, NoneAllowed=False, AllAllowed=True):
    """Ask the user to pick one or more value(s) in a iterable (list,
    set). Return the list of chosen values"""
    res = None

    while res == None:
      try:
        self.print_iterable(iterable, NoneAllowed, AllAllowed)
        res = [int(i) for i in raw_input(self.prompt).split()]
      except ValueError:
        pass

    if AllAllowed and (len(iterable) in res or res == []):
      return iterable
    elif NoneAllowed and res == [-1]:
      return []
    else:
      return [iterable[i] for i in res if i in range(0, len(iterable))]

  def print_iterable(self, iterable, NoneAllowed=False, AllAllowed=True):
    """Prints an iterable with numbering to enable a user to pick some
    or all elements by typing the numbers. To be used by an input function.
    """
    if NoneAllowed:
      print "(-1) [Nothing]"
    for idx, value in enumerate(iterable):
      print "(%d) [%s]" % (idx, value)
    if AllAllowed:
      print "(%d) [All] (default)" % len(iterable)
