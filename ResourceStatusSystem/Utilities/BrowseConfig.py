"""
Access RSSConfiguration from CS easily.
"""
from DIRAC import gConfig
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import RSSException

rssConfigRootPath = "/Operations/RSSConfiguration/"

def getOptions(path):
  """Gives the options of a CS section in a Python dict with values as
  lists and converted into integers if needed"""

  pathdict = gConfig.getOptionsDict(path)
  try:
    pathdict = pathdict['Value']
  except KeyError:
    raise RSSException, pathdict['Message']

  for k in pathdict:
    pathdict[k] = pathdict[k].split(',')
    try:
      pathdict[k] = [int(i) for i in pathdict[k]]
      if len(pathdict[k]) == 1:
        pathdict[k] = pathdict[k][0]
    except ValueError:
      pass

  return pathdict

def getSections(path):
  """Gives the sections in a CS section as a Python list"""
  seclist = gConfig.getSections(path)
  try:
    return seclist['Value']
  except KeyError:
    raise RSSException, seclist['Message']

def getDictRootedAt(relpath = "", root = rssConfigRootPath):
  """Gives the configuration rooted at path in a Python dict. The
  result is a Python dictionnary that reflects the structure of the
  config file."""
  def getDictRootedAt(path):
    retval = {}
    opts = getOptions(path)
    secs = getSections(path)
    for k in opts:
      retval[k] = opts[k]
    for i in secs:
      retval[i] = getDictRootedAt(path + "/" + i)
    return retval

  return getDictRootedAt(root + relpath)

if __name__ == "__main__":
  import sys
  from DIRAC.Core.Base import Script
  Script.parseCommandLine(ignoreErrors = True)

  if len(sys.argv) > 1:
    print getDictRootedAt(sys.argv[1])
  else:
    print getDictRootedAt()
