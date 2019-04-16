import tempfile
import os
import subprocess
import gzip
import shutil
import sys

from DIRAC import gLogger, S_OK, S_ERROR, rootPath
from DIRAC.ConfigurationSystem.Client.Helpers.CSGlobals import getInstalledExtensions
from DIRAC.Core.HTTP.Lib.SessionData import SessionData
from DIRAC.Core.HTTP.Core.HandlerMgr import HandlerMgr
from DIRAC.Core.HTTP.Lib.CompilerHelper import CompilerHelper
from DIRAC.Core.Utilities.Decorators import deprecated


class Compiler(object):

  def __init__(self):
    self.__extVersion = SessionData.getExtJSVersion()
    self.__staticPaths = HandlerMgr().getPaths("static")
    self.__extensions = getInstalledExtensions()
    self.__webAppPath = os.path.dirname(self.__staticPaths[-1])
    self.__extPath = os.path.join(self.__webAppPath, "static", "extjs", self.__extVersion)
    self.__sdkPath = os.path.join(self.__webAppPath, "static", "extjs", self.__extVersion, "src")
    self.__appDependency = CompilerHelper().getAppDependencies()

    self.__classPaths = [os.path.join(self.__webAppPath, *p) for p in (("static", "core", "js", "utils"),
                                                                       ("static", "core", "js", "core"))]
    self.__classPaths.append(os.path.join(self.__extPath, "examples", "ux", "form"))
    self.__classPaths.append(os.path.join(self.__extPath, "examples", "ux"))

    self.__debugFlag = str(gLogger.getLevel() in ('DEBUG', 'VERBOSE', 'INFO')).lower()
    self.__inDir = os.path.join(os.path.dirname(self.__webAppPath), "Lib", "CompileTemplates")

    self.__senchacmddir = os.path.join(rootPath, "sbin", "Sencha", "Cmd")
    self.__senchaVersion = "v6.5.0.180"

  def __writeINFile(self, tplName, extra=False):
    inTpl = os.path.join(self.__inDir, tplName)
    try:
      with open(inTpl) as infd:
        data = infd.read()
    except IOError:
      return S_ERROR("%s does not exist" % inTpl)
    data = data.replace("%EXT_VERSION%", self.__extVersion)
    if extra:
      for k in extra:
        data = data.replace("%%%s%%" % k.upper(), extra[k])
    outfd, filepath = tempfile.mkstemp(".compilejs.%s" % tplName)
    os.write(outfd, data)
    os.close(outfd)
    return S_OK(filepath)

  def __cmd(self, cmd):
    env = {}
    # MEGAHACK FOR FUCKING OSX LION
    for k in ('LD_LIBRARY_PATH', 'DYLD_LIBRARY_PATH'):
      if k in os.environ:
        env[k] = os.environ[k]
        os.environ.pop(k)
    gLogger.verbose("Command is: %s" % " ".join(cmd))
    result = subprocess.call(cmd)
    for k in env:
      os.environ[k] = env[k]
    return result

  def __compileApp(self, extPath, extName, appName, extClassPath=""):
    result = self.__writeINFile("app.tpl", {'APP_LOCATION': '%s.%s.classes.%s' % (extName, appName, appName)})
    if not result['OK']:
      return result
    inFile = result['Value']
    buildDir = os.path.join(extPath, appName, 'build')
    try:
      shutil.rmtree(buildDir)
    except OSError:
      pass
    if not os.path.isdir(buildDir):
      try:
        os.makedirs(buildDir)
      except IOError as excp:
        return S_ERROR("Can't create build dir %s" % excp)
    outFile = os.path.join(buildDir, "index.html")
    compressedJsFile = os.path.join(buildDir, appName + '.js')

    classPath = list(self.__classPaths)
    excludePackage = ",%s.*" % extName
    if extClassPath != "":
      classPath.append(extClassPath)
      excludePackage = ",DIRAC.*,%s.*" % extName

    classPath.append(os.path.join(extPath, appName, "classes"))

    cmd = ['sencha', '-sdk', self.__sdkPath, 'compile', '-classpath=%s' % ",".join(classPath),
           '-debug=%s' % self.__debugFlag, 'page', '-name=page', '-input-file', inFile, '-out', outFile, 'and',
           'restore', 'page', 'and', 'exclude', '-not', '-namespace', 'Ext.dirac.*%s' % excludePackage, 'and',
           'concat', '-yui', compressedJsFile]

    if self.__cmd(cmd):
      return S_ERROR("Error compiling %s.%s" % (extName, appName))

    return S_OK()

  def __zip(self, staticPath, stack=""):
    c = 0
    l = "|/-\\"
    for entry in os.listdir(staticPath):
      n = stack + l[c % len(l)]
      if entry[-3:] == ".gz":
        continue
      ePath = os.path.join(staticPath, entry)
      if os.path.isdir(ePath):
        self.__zip(ePath, n)
        continue
      zipPath = "%s.gz" % ePath
      if os.path.isfile(zipPath):
        if os.stat(zipPath).st_mtime > os.stat(ePath).st_mtime:
          continue
      print "%s%s\r" % (n, " " * (20 - len(n))),
      c += 1
      inf = gzip.open(zipPath, "wb", 9)
      with open(ePath, "rb") as outf:
        buf = outf.read(8192)
        while buf:
          inf.write(buf)
          buf = outf.read(8192)
      inf.close()

  @deprecated("Only for extjs4. Do not use this command any more")
  def run(self):

    # if the sencha does not installed, it will exit
    self.__checkSenchacmd()

    staticPath = os.path.join(self.__webAppPath, "static")
    gLogger.notice("Compiling core")

    result = self.__writeINFile("core.tpl")
    if not result['OK']:
      return result
    inFile = result['Value']
    buildDir = os.path.join(staticPath, "core", "build")
    try:
      shutil.rmtree(buildDir)
    except OSError:
      pass
    outFile = os.path.join(staticPath, "core", "build", "index.html")
    gLogger.verbose(" IN file written to %s" % inFile)

    cmd = ['sencha', '-sdk', self.__sdkPath, 'compile', '-classpath=%s' % ",".join(self.__classPaths),
           '-debug=%s' % self.__debugFlag, 'page', '-yui', '-input-file', inFile, '-out', outFile]

    if self.__cmd(cmd):
      gLogger.error("Error compiling JS")
      return S_ERROR("Failed compiling core")

    try:
      os.unlink(inFile)
    except IOError:
      pass

    for staticPath in self.__staticPaths:
      gLogger.notice("Looing into %s" % staticPath)
      for extName in self.__extensions:
        if extName != 'DIRAC':  # if we have a VO specific installation we have to discover the extension name.
          # For example: self.__extensions=['DIRAC','LHCbWebDIRAC'] and staticPath = '../LHCbWebDIRAC/WebApp/static'
          # extPath='../LHCbWebDIRAC/WebApp/static/LHCbWebDIRAC' this directory does not exits because we call it LHCbDIRAC.
          # In that case the extPath='../LHCbWebDIRAC/WebApp/static/LHCbDIRAC'
          extDirectoryContent = os.listdir(staticPath)
          if len(extDirectoryContent) == 0:
            return S_ERROR("The exstension directory is empty:" + str(staticPath))
          else:
            extName = extDirectoryContent[-1]
            gLogger.notice("Detected extension:%s" % extName)

        extPath = os.path.join(staticPath, extName)
        if not os.path.isdir(extPath):
          continue
        gLogger.notice("Exploring %s" % extName)
        for appName in os.listdir(extPath):
          expectedJS = os.path.join(extPath, appName, "classes", "%s.js" % appName)
          if not os.path.isfile(expectedJS):
            continue
          classPath = self.__getClasspath(extName, appName)
          gLogger.notice("Trying to compile %s.%s.classes.%s CLASSPATH=%s" % (extName, appName, appName, classPath))
          result = self.__compileApp(extPath, extName, appName, classPath)
          if not result['OK']:
            return result

    gLogger.notice("Zipping static files")
    self.__zip(staticPath)
    gLogger.notice("Done")
    return S_OK()

  def __getClasspath(self, extName, appName):

    classPath = ''
    dependency = self.__appDependency.get("%s.%s" % (extName, appName), "")

    if dependency != "":
      depPath = dependency.split(".")
      for staticPath in self.__staticPaths:
        expectedJS = os.path.join(staticPath, depPath[0], depPath[1], "classes")
        print expectedJS
        if not os.path.isdir(expectedJS):
          continue
        classPath = expectedJS
    return classPath

  def __checkSenchacmd(self):

    try:
      path = os.path.join(self.__senchacmddir, self.__senchaVersion)
      if os.path.exists(path):
        sys.path.append(path)
        syspath = os.environ['PATH']
        os.environ['PATH'] = path + os.pathsep + syspath

      self.__cmd(["sencha"])
    except OSError:
      raise OSError("sencha cmd is not installed!")
