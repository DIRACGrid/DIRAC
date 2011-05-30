"""
Access RSSConfiguration from CS easily.
"""

from DIRAC.ConfigurationSystem.private.Modificator      import Modificator
import itertools

class BrowseConfig(Modificator):

  rssConfigRootPath = "/Operations/RSSConfiguration"

  def getOptionsDict(self, path):
    """Gives the options of a CS section in a Python dict with values as
    lists and converted into integers if needed"""

    opts = self.getOptions(path)
    vals = [self.getValue(path + "/" + o) for o in opts]
    pathdict = dict(itertools.imap(None, opts, vals))

    for k in pathdict:
      pathdict[k] = pathdict[k].split(',')
      try:               pathdict[k] = [int(i) for i in pathdict[k]]
      except ValueError: pass
      if len(pathdict[k]) == 1:
        pathdict[k] = pathdict[k][0]

    return pathdict

  def getDictRootedAt(self, relpath = "", root = rssConfigRootPath):
    """Gives the configuration rooted at path in a Python dict. The
    result is a Python dictionnary that reflects the structure of the
    config file."""
    def getDictRootedAt(path):
      retval = {}
      opts = self.getOptions(path)
      secs = self.getSections(path)
      for k in opts:
        retval[k] = opts[k]
      for i in secs:
        retval[i] = getDictRootedAt(path + "/" + i)
      return retval

    return getDictRootedAt(root + "/" + relpath)
