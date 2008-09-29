########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/Utilities/ldLibraryPath.py,v 1.3 2008/09/29 14:24:20 rgracian Exp $
# File :   ldLibraryPath
# Author : Ricardo Graciani
########################################################################
__RCSID__   = "$Id: ldLibraryPath.py,v 1.3 2008/09/29 14:24:20 rgracian Exp $"
__VERSION__ = "$Revision: 1.3 $"
"""
  Utilities to handle issues with LD_LIBRARY_PATH
"""
from DIRAC import platformTuple
import os

def unify( path, newpath ):
  # for Linux and MacOS link or files in pathList in a single directory path.
  # For that we go a long pathList in reverse order and link all files
  # from a path, the latest appearence of a file will take precedence
  if not platformTuple[0] == 'Windows':
    if os.path.exists( newpath ):
      if not os.path.isdir(newpath):
        try:
          os.remove( newpath )
        except:
          return path
    else:
      try:
        os.makedirs( newpath )
      except:
        return path
    pathList = path.split(':')
    for dummy in pathList[:]:
      ldDir = pathList.pop()
      if not os.path.isdir( ldDir ):
        continue
      ldLibs = os.listdir( ldDir )
      for f in ldLibs:
        newF = os.path.join( newpath,f )
        ldF  = os.path.join( ldDir, f)
        # 1. Check if the file exist (broken links will return False)
        if os.path.isfile( ldF ):
          ldF = os.path.realpath( ldF )
          # 2. Check if the link is present already
          if os.path.exists( newF ):
            # 3. Check is the point to the same file
            if os.path.samefile( newF, ldF ):
              continue
            else:
              os.remove( newF )
          # 4. Create the link
          os.symlink( ldF, newF )
    return newpath
  else:
    # Windows does nothing for the moment
    return path