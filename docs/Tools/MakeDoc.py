#!/bin/env python
""" create rst files for documentation of DIRAC """
import os
import shutil
import socket
import sys


def mkdir(folder):
  """create a folder, ignore if it exists"""
  try:
    folder = os.path.join(os.getcwd(), folder)
    os.mkdir(folder)
  except OSError as e:
    print "MakeDoc: Exception %s when creating folder" % repr(e), folder


BASEPATH = "docs/source/CodeDocumentation"
DIRACPATH = os.environ.get("DIRAC", "") + "/DIRAC"

ORIGDIR = os.getcwd()

BASEPATH = os.path.join(DIRACPATH, BASEPATH)

# files that call parseCommandLine or similar issues
BAD_FILES = ("lfc_dfc_copy",
             "lfc_dfc_db_copy",
             "JobWrapperTemplate",
             "PlotCache",  # PlotCache creates a thread on import, which keeps sphinx from exiting
             "PlottingHandler",
             "setup.py",  # configuration for style check
             # "DataStoreClient", # instantiates itself
             # "ReportsClient", ## causes gDataCache to start
             # "ComponentInstaller", # tries to connect to a DB
             # "ProxyDB", # tries to connect to security log server
             # "SystemAdministratorHandler", # tries to connect to monitoring
             # "GlobusComputingElement", # tries to connect to a DB
             # "HTCondorCEComputingElememt", # tries to connect to a DB
             # "TaskManager", #Tries to connect to security logging
             )

FORCE_ADD_PRIVATE = ["FCConditionParser"]


def mkRest(filename, modulename, fullmodulename, subpackages=None, modules=None):
  """make a rst file for filename"""
  if modulename == "scripts":
    return
  else:
    modulefinal = modulename

  lines = []
  lines.append("%s" % modulefinal)
  lines.append("=" * len(modulefinal))
  lines.append(".. module:: %s " % fullmodulename)
  lines.append("")

  if subpackages or modules:
    lines.append(".. toctree::")
    lines.append("   :maxdepth: 1")
    lines.append("")

  subpackages = [s for s in subpackages if not s.endswith(("scripts", ))]
  if subpackages:
    print "MakeDoc: ", modulename, " subpackages ", subpackages
    lines.append("SubPackages")
    lines.append("...........")
    lines.append("")
    lines.append(".. toctree::")
    lines.append("   :maxdepth: 1")
    lines.append("")
    for package in sorted(subpackages):
      lines.append("   %s/%s_Module.rst" % (package, package.split("/")[-1]))
      #lines.append("   %s " % (package, ) )
    lines.append("")

  # remove CLI etc. because we drop them earlier
  modules = [m for m in modules if not m.endswith("CLI") and "-" not in m]
  if modules:
    lines.append("Modules")
    lines.append(".......")
    lines.append("")
    lines.append(".. toctree::")
    lines.append("   :maxdepth: 1")
    lines.append("")
    for module in sorted(modules):
      lines.append("   %s.rst" % (module.split("/")[-1],))
      #lines.append("   %s " % (package, ) )
    lines.append("")

  with open(filename, 'w') as rst:
    rst.write("\n".join(lines))


def mkDummyRest(classname, fullclassname):
  """ create a dummy rst file for files that behave badly """
  filename = classname + ".rst"

  lines = []
  lines.append("%s" % classname)
  lines.append("=" * len(classname))
  lines.append("")
  lines.append(" This is an empty file, because we cannot parse this file correctly or it causes problems")
  lines.append(" , please look at the source code directly")
  with open(filename, 'w') as rst:
    rst.write("\n".join(lines))


def mkModuleRest(classname, fullclassname, buildtype="full"):
  """ create rst file for class"""
  filename = classname + ".rst"

  lines = []
  lines.append("%s" % classname)
  lines.append("=" * len(classname))

  # if "-" not in classname:
  #   lines.append(".. autosummary::" )
  #   lines.append("   :toctree: %sGen" % classname )
  #   lines.append("")
  #   lines.append("   %s " % fullclassname )
  #   lines.append("")

  lines.append(".. automodule:: %s" % fullclassname)
  if buildtype == "full":
    lines.append("   :members:")
    lines.append("   :inherited-members:")
    lines.append("   :undoc-members:")
    lines.append("   :show-inheritance:")
    if classname in FORCE_ADD_PRIVATE:
      lines.append("   :special-members:")
      lines.append("   :private-members:")
    else:
      lines.append("   :special-members: __init__")
    if classname.startswith("_"):
      lines.append("   :private-members:")

  with open(filename, 'w') as rst:
    rst.write("\n".join(lines))


def getsubpackages(abspath, direc):
  """return list of subpackages with full path"""
  packages = []
  for dire in direc:
    if dire.lower() == "test" or dire.lower() == "tests" or "/test" in dire.lower():
      print "MakeDoc: skipping this directory", dire
      continue
    if os.path.exists(os.path.join(DIRACPATH, abspath, dire, "__init__.py")):
      #packages.append( os.path.join( "DOC", abspath, dire) )
      packages.append(os.path.join(dire))
  return packages


def getmodules(_abspath, _direc, files):
  """return list of subpackages with full path"""
  packages = []
  for filename in files:
    if "test" in filename.lower():
      print "MakeDoc: Skipping this file", filename
      continue
    if filename != "__init__.py":
      packages.append(filename.split(".py")[0])

  return packages


def createDoc(buildtype="full"):
  """create the rst files for all the things we want them for"""
  print "MakeDoc: DIRACPATH", DIRACPATH
  print "MakeDoc: BASEPATH", BASEPATH
  print "Host", socket.gethostname()

  # we need to replace existing rst files so we can decide how much code-doc to create
  if os.path.exists(BASEPATH):
    shutil.rmtree(BASEPATH)
  mkdir(BASEPATH)
  os.chdir(BASEPATH)
  print "MakeDoc: Now creating rst files"
  for root, direc, files in os.walk(DIRACPATH):
    configTemplate = [os.path.join(root, _) for _ in files if _ == 'ConfigTemplate.cfg']
    files = [_ for _ in files if _.endswith(".py")]

    if "__init__.py" not in files:
      continue

    if any(root.lower().endswith(f.lower()) for f in ("/docs", )):
      continue
    elif any(f.lower() in root.lower() for f in ("/test", "scripts",
                                                 )):
      print "MakeDoc: Skipping this folder:", root
      continue

    modulename = root.split("/")[-1]
    abspath = root.split(DIRACPATH)[1].strip("/")
    fullmodulename = 'DIRAC.' + '.'.join(abspath.split('/'))
    packages = getsubpackages(abspath, direc)
    if abspath:
      mkdir(abspath)
      os.chdir(abspath)
    if modulename == "DIRAC":
      createCodeDocIndex(
          subpackages=packages,
          modules=getmodules(
              abspath,
              direc,
              files),
          buildtype=buildtype)
    elif buildtype == "limited":
      os.chdir(BASEPATH)
      return 0
    else:
      mkRest(
          modulename + "_Module.rst",
          modulename,
          fullmodulename,
          subpackages=packages,
          modules=getmodules(
              abspath,
              direc,
              files))

    for filename in files:
      # Skip things that call parseCommandLine or similar issues
      fullclassname = ".".join(abspath.split("/") + [filename])
      if any(f in filename for f in BAD_FILES):
        mkDummyRest(filename.split(".py")[0], fullclassname.split(".py")[0])
        continue
      elif not filename.endswith(".py") or \
              filename.endswith("CLI.py") or \
              filename.lower().startswith("test") or \
              filename == "__init__.py" or \
              "-" in filename:  # not valid python identifier, e.g. dirac-pilot
        continue
      if not fullclassname.startswith("DIRAC."):
        fullclassname = "DIRAC." + fullclassname
      # Remove some FrameworkServices because things go weird
      mkModuleRest(filename.split(".py")[0], fullclassname.split(".py")[0], buildtype)

    if configTemplate:
      shutil.copy(configTemplate[0], os.path.join(BASEPATH, abspath))

    os.chdir(BASEPATH)

  shutil.copy(os.path.join(DIRACPATH, 'dirac.cfg'), BASEPATH)

  return 0


def createCodeDocIndex(subpackages, modules, buildtype="full"):
  """create the main index file"""
  filename = "index.rst"
  lines = []
  lines.append(".. _code_documentation:")
  lines.append("")
  lines.append("Code Documentation (|release|)")
  lines.append("------------------------------")

  # for limited builds we only create the most basic code documentation so
  # we let users know there is more elsewhere
  if buildtype == "limited":
    lines.append("")
    lines.append(".. warning::")
    lines.append(
        "  This a limited build of the code documentation, for the full code documentation please look at the website")
    lines.append("")
  else:
    if subpackages or modules:
      lines.append(".. toctree::")
      lines.append("   :maxdepth: 1")
      lines.append("")

    if subpackages:
      systemPackages = sorted([pck for pck in subpackages if pck.endswith("System")])
      otherPackages = sorted([pck for pck in subpackages if not pck.endswith("System")])

      lines.append("=======")
      lines.append("Systems")
      lines.append("=======")
      lines.append("")
      lines.append(".. toctree::")
      lines.append("   :maxdepth: 1")
      lines.append("")
      for package in systemPackages:
        lines.append("   %s/%s_Module.rst" % (package, package.split("/")[-1]))

      lines.append("")
      lines.append("=====")
      lines.append("Other")
      lines.append("=====")
      lines.append("")
      lines.append(".. toctree::")
      lines.append("   :maxdepth: 1")
      lines.append("")
      for package in otherPackages:
        lines.append("   %s/%s_Module.rst" % (package, package.split("/")[-1]))

    if modules:
      for module in sorted(modules):
        lines.append("   %s.rst" % (module.split("/")[-1],))
        #lines.append("   %s " % (package, ) )

  with open(filename, 'w') as rst:
    rst.write("\n".join(lines))


def checkBuildTypeAndRun():
  """ check for input argument and then create the doc rst files """
  buildtypes = ("full", "limited")
  buildtype = "full" if len(sys.argv) <= 1 else sys.argv[1]
  if buildtype not in buildtypes:
    print "MakeDoc: Unknown build type: %s use %s " % (buildtype, " ".join(buildtypes))
    return 1
  print "MakeDoc: buildtype:", buildtype
  exit(createDoc(buildtype))


if __name__ == "__main__":
  # get the options
  exit(checkBuildTypeAndRun())
