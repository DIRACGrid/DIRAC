# $HeadURL:  $

__RCSID__ = "$Id: $"

from DIRAC import gConfig, S_OK, S_ERROR

def getCurrentVersion():
  """ Get a string corresponding to the current version of the DIRAC package and all the installed
      extension packages
  """
  
  import DIRAC
  version = 'DIRAC '+DIRAC.version
  
  extensions = gConfig.getOption('/DIRAC/Extensions',[])
  for e in extensions['Value']:
    try: 
      exec "import %sDIRAC" % e
      version = "%sDIRAC " % e + eval('%sDIRAC.version' % e) +'; '+ version
    except ImportError:
      pass
    except AttributeError:
      pass
    
  return S_OK(version)  

def getVersion():
  """ Get a dictionary corresponding to the current version of the DIRAC package and all the installed
      extension packages
  """
  
  import DIRAC
  vDict = {'Extensions':{}}
  vDict['DIRAC'] = DIRAC.version
  
  extensions = gConfig.getOption('/DIRAC/Extensions',[])
  for e in extensions['Value']:
    try: 
      exec "import %sDIRAC" % e
      version = eval('%sDIRAC.version' % e)
      vDict['Extensions'][e] = version
    except ImportError:
      pass
    except AttributeError:
      pass
    
  return S_OK(vDict)  
  