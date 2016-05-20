#!/usr/bin/env python
""" create rst files for documentation of DIRAC """
import os

import fakeEnvironment
import fakeEnv

def mkdir( folder ):
  """create a folder, ignore if it exists"""
  try:
    folder = os.path.join(os.getcwd(),folder)
    os.mkdir( folder )
  except OSError as e:
    print "Exception for",folder,repr(e)


BASEPATH = "docs/source/CodeDocumentation"
DIRACPATH = os.environ.get("DIRAC") + "/DIRAC"

ORIGDIR = os.getcwd()

BASEPATH = os.path.join( DIRACPATH, BASEPATH )

def mkRest( filename, modulename, fullmodulename, subpackages=None, modules=None ):
  """make a rst file for filename"""
  if modulename == "scripts":
    return
    #modulefinal = fullmodulename.split(".")[-2]+" Scripts"
  else:
    modulefinal = modulename

  print fullmodulename, "\n++subpackages",subpackages, "\n++Modules",modules
  lines = []
  lines.append("%s" % modulefinal)
  lines.append("="*len(modulefinal))
  lines.append(".. module:: %s " % fullmodulename )
  lines.append("" )

  if subpackages or modules:
    lines.append(".. toctree::")
    lines.append("   :maxdepth: 1")
    lines.append("")

  subpackages = [ s for s in subpackages if not s.endswith( ("scripts", ) ) ]
  if subpackages:
    print modulename, " subpackages ", subpackages
    lines.append( "SubPackages" )
    lines.append( "..........." )
    lines.append( "" )
    lines.append(".. toctree::")
    lines.append("   :maxdepth: 1")
    lines.append("")
    for package in sorted(subpackages):
      lines.append("   %s/%s_Module.rst" % (package,package.split("/")[-1] ) )
      #lines.append("   %s " % (package, ) )

  ##remove CLI because we drop them earlier
  modules = [ m for m in modules if not m.endswith("CLI") ]
  if modules:
    lines.append( "Modules" )
    lines.append( "......." )
    lines.append( "" )
    lines.append(".. toctree::")
    lines.append("   :maxdepth: 1")
    lines.append("")
    for module in sorted(modules):
      lines.append("   %s.rst" % (module.split("/")[-1],) )
      #lines.append("   %s " % (package, ) )

  with open(filename, 'w') as rst:
    rst.write("\n".join(lines))

    
def mkModuleRest( classname, fullclassname ):
  """ create rst file for class"""
  filename = classname+".rst"

  lines = []
  lines.append("%s" % classname)
  lines.append("="*len(classname))

  # if "-" not in classname:
  #   lines.append(".. autosummary::" )
  #   lines.append("   :toctree: %sGen" % classname )
  #   lines.append("")
  #   lines.append("   %s " % fullclassname )
  #   lines.append("")

  lines.append(".. automodule:: %s" % fullclassname )
  lines.append("   :members:" )
  lines.append("   :inherited-members:" )
  lines.append("   :undoc-members:" )
  lines.append("   :show-inheritance:" )
  if classname.startswith("_"):
    lines.append( "   :private-members:" )

  with open(filename, 'w') as rst:
    rst.write("\n".join(lines))

  
def getsubpackages( abspath, direc):
  """return list of subpackages with full path"""
  packages = []
  for dire in direc:
    if "test" in dire.lower():
      continue
    #print os.path.join( DIRACPATH,abspath,dire, "__init__.py" )
    if os.path.exists( os.path.join( DIRACPATH,abspath,dire, "__init__.py" ) ):
      #packages.append( os.path.join( "DOC", abspath, dire) )
      packages.append( os.path.join( dire ) )
  #print "packages",packages
  return packages

def getmodules( _abspath, _direc, files ):
  """return list of subpackages with full path"""
  packages = []
  for filename in files:
    if "test" in filename.lower():
      continue
    if filename != "__init__.py":
      packages.append( filename.split(".py")[0] )

  return packages


def createDoc():
  """create the rst files for all the things we want them for"""
  print "DIRACPATH",DIRACPATH
  print "BASEPATH", BASEPATH
  mkdir(BASEPATH)
  os.chdir(BASEPATH)
  
  for root,direc,files in os.walk(DIRACPATH):
    files = [ _ for _ in files if _.endswith(".py") ]
    if "__init__.py" not in files:
      continue

    if any( root.lower().endswith( f.lower() ) for f in ("/docs", ) ):
      #print "Skipping:", root
      continue
    elif any( f.lower() in root.lower() for f in ("test", "scripts",
                                                 ) ):
      continue
    
    #print root, direc, files
    modulename = root.split("/")[-1]
    abspath = root.split(DIRACPATH)[1].strip("/")
    fullmodulename = ".".join(abspath.split("/"))
    packages = getsubpackages(abspath,direc)
    #print "packages for ", root, packages
    if abspath:
      mkdir( abspath )
      os.chdir( abspath )
    #print "Making rst",modulename
    if modulename == "DIRAC":
      createCodeDocIndex(subpackages=packages, modules=getmodules(abspath, direc, files))
    else:
      mkRest( modulename+"_Module.rst", modulename, fullmodulename, subpackages=packages, modules=getmodules(abspath, direc, files) )

    for filename in files:
      if filename.lower().startswith("test"):
        continue
      ## Skip things that call parseCommandLine or similar issues
      if any( f in filename for f in ("lfc_dfc_copy", "lfc_dfc_db_copy", "JobWrapperTemplate", "Refresher") ):
        continue
      if filename.endswith("CLI.py"):
        continue
      if filename == "__init__.py":
        continue
      if not filename.endswith(".py"):
        continue
      fullclassname = ".".join(abspath.split("/")+[filename])
      if not fullclassname.startswith( "DIRAC." ):
        fullclassname = "DIRAC."+fullclassname
      mkModuleRest( filename.split(".py")[0], fullclassname.split(".py")[0] )

    os.chdir(BASEPATH)
  return 0

def createCodeDocIndex( subpackages, modules):
  """create the main index file"""
  filename = "index.rst"
  lines = []
  lines.append( ".. _code_documentation:")
  lines.append("")
  lines.append( "Code Documentation (|release|)" )
  lines.append( "------------------------------" )
                

  if subpackages or modules:
    lines.append(".. toctree::")
    lines.append("   :maxdepth: 1")
    lines.append("")

  if subpackages:
    systemPackages = sorted([ pck for pck in subpackages if pck.endswith("System") ])
    otherPackages = sorted([ pck for pck in subpackages if not pck.endswith("System") ])

    lines.append( "=======" )
    lines.append( "Systems" )
    lines.append( "=======" )
    lines.append("")
    lines.append(".. toctree::")
    lines.append("   :maxdepth: 1")
    lines.append("")
    for package in systemPackages:
      lines.append("   %s/%s_Module.rst" % (package,package.split("/")[-1] ) )

    lines.append("")
    lines.append( "=====" )
    lines.append( "Other" )
    lines.append( "=====" )
    lines.append("")
    lines.append(".. toctree::")
    lines.append("   :maxdepth: 1")
    lines.append("")
    for package in otherPackages:
      lines.append("   %s/%s_Module.rst" % (package,package.split("/")[-1] ) )

  if modules:
    for module in sorted(modules):
      lines.append("   %s.rst" % (module.split("/")[-1],) )
      #lines.append("   %s " % (package, ) )

  with open(filename, 'w') as rst:
    rst.write("\n".join(lines))
  
if __name__ == "__main__":
  exit(createDoc())
