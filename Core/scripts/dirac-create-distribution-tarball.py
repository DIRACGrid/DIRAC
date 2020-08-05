#!/usr/bin/env python
# -*- coding: utf-8 -*-
########################################################################
# File :    dirac-create-distribution-tarball
# Author :  Adria Casajus
########################################################################
"""
  Create tarballs for a given DIRAC release
"""
import sys
import os
import shutil
import tempfile
import subprocess32 as subprocess
import shlex

from DIRAC.Core.Utilities.File import mkDir
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities import Distribution, Subprocess


__RCSID__ = "$Id$"


class TarModuleCreator(object):

  VALID_VCS = ('svn', 'git', 'hg', 'file')

  class Params(object):

    def __init__(self):
      self.version = False
      self.destination = False
      self.sourceURL = False
      self.name = False
      self.vcs = False
      self.vcsBranch = False
      self.vcsPath = False
      self.relNotes = False
      self.outRelNotes = False
      self.extensionVersion = None
      self.extensionSource = None
      self.extjspath = None

    def isOK(self):
      if not self.version:
        return S_ERROR("No version defined")
      if not self.sourceURL:
        return S_ERROR("No Source URL defined")
      if not self.name:
        return S_ERROR("No name defined")
      if self.vcs and self.vcs not in TarModuleCreator.VALID_VCS:
        return S_ERROR("Invalid VCS %s" % self.vcs)
      return S_OK()

    def setVersion(self, opVal):
      self.version = opVal
      return S_OK()

    def setDestination(self, opVal):
      self.destination = os.path.realpath(opVal)
      return S_OK()

    def setSourceURL(self, opVal):
      self.sourceURL = opVal
      return S_OK()

    def setName(self, opVal):
      self.name = opVal
      return S_OK()

    def setVCS(self, opVal):
      self.vcs = opVal.lower()
      if self.vcs == 'subversion':
        self.vcs = 'svn'
      elif self.vcs == 'mercurial':
        self.vcs = 'hg'
      return S_OK()

    def setVCSBranch(self, opVal):
      self.vcsBranch = opVal
      return S_OK()

    def setVCSPath(self, opVal):
      self.vcsPath = opVal
      return S_OK()

    def setReleaseNotes(self, opVal):
      self.relNotes = opVal
      return S_OK()

    def setOutReleaseNotes(self, opVal):
      self.outRelNotes = True
      return S_OK()

    def setExtensionVersion(self, opVal):
      self.extensionVersion = opVal
      return S_OK()

    def setExtensionSource(self, opVal):
      self.extensionSource = opVal
      return S_OK()

    def setExtJsPath(self, opVal):
      self.extjspath = opVal
      return S_OK()

  def __init__(self, params):
    self.params = params

  def __checkDestination(self):
    if not self.params.destination:
      self.params.destination = tempfile.mkdtemp('DIRACTarball')

    gLogger.notice("Will generate tarball in %s" % self.params.destination)
    mkDir(self.params.destination)
    return S_OK()

  def __discoverVCS(self):
    sourceURL = self.params.sourceURL
    if os.path.expanduser(sourceURL).find("/") == 0:
      sourceURL = os.path.expanduser(sourceURL)
      self.params.vcs = "file"
      return True
    if sourceURL.find(".git") == len(sourceURL) - 4:
      self.params.vcs = "git"
      return True
    for vcs in TarModuleCreator.VALID_VCS:
      if sourceURL.find(vcs) == 0:
        self.params.vcs = vcs
        return True
    return False

  def __checkoutSource(self, moduleName=None, sourceURL=None, tagVersion=None):
    """
    This method will checkout a given module from a given repository: svn, hg, git

    :param str moduleName: The name of the Module: for example: LHCbWebDIRAC
    :param str sourceURL: The code repository: ssh://git@gitlab.cern.ch:7999/lhcb-dirac/LHCbWebDIRAC.git
    :param str tagVersion: the tag for example: v4r3p6
    """
    if not self.params.vcs:
      if not self.__discoverVCS():
        return S_ERROR("Could not autodiscover VCS")
    gLogger.info("Checking out using %s method" % self.params.vcs)

    if self.params.vcs == "file":
      return self.__checkoutFromFile(moduleName, sourceURL)
    elif self.params.vcs == "svn":
      return self.__checkoutFromSVN(moduleName, sourceURL, tagVersion)
    elif self.params.vcs == "hg":
      return self.__checkoutFromHg(moduleName, sourceURL)
    elif self.params.vcs == "git":
      return self.__checkoutFromGit(moduleName, sourceURL, tagVersion)

    return S_ERROR("OOPS. Unknown VCS %s!" % self.params.vcs)

  def __checkoutFromFile(self, moduleName=None, sourceURL=None):
    """
    This method checkout a given tag from a file
    Note: we can checkout any project form a file

    :param str moduleName: The name of the Module
    :param str sourceURL: The code repository

    """
    if not moduleName:
      moduleName = self.params.name

    if not sourceURL:
      sourceURL = self.params.sourceURL

    if sourceURL.find("file://") == 0:
      sourceURL = sourceURL[7:]
    sourceURL = os.path.realpath(sourceURL)
    try:
      pyVer = sys.version_info
      if pyVer[0] == 2 and pyVer[1] < 6:
        shutil.copytree(sourceURL,
                        os.path.join(self.params.destination, moduleName),
                        symlinks=True)
      else:
        shutil.copytree(sourceURL,
                        os.path.join(self.params.destination, moduleName),
                        symlinks=True,
                        ignore=shutil.ignore_patterns('.svn', '.git', '.hg', '*.pyc', '*.pyo'))
    except Exception as e:
      return S_ERROR("Could not copy data from source URL: %s" % str(e))
    return S_OK()

  def __checkoutFromSVN(self, moduleName=None, sourceURL=None, tagVersion=None):
    """
    This method checkout a given tag from a SVN repository.
    Note: we can checkout any project form a SVN repository

    :param str moduleName: The name of the Module
    :param str sourceURL: The code repository
    :param str tagVersion: the tag for example: v4r3p6

    """

    if not moduleName:
      moduleName = self.params.name

    if not sourceURL:
      sourceURL = self.params.sourceURL

    if not tagVersion:
      tagVersion = self.params.version

    cmd = "svn export --trust-server-cert --non-interactive '%s/%s' '%s'" % (sourceURL, tagVersion,
                                                                             os.path.join(self.params.destination,
                                                                                          moduleName))
    gLogger.verbose("Executing: %s" % cmd)
    result = Subprocess.systemCall(900, shlex.split(cmd))
    if not result['OK']:
      return S_ERROR("Error while retrieving sources from SVN: %s" % result['Message'])
    exitStatus, stdData, errData = result['Value']
    if exitStatus:
      return S_ERROR("Error while retrieving sources from SVN: %s" % "\n".join([stdData, errData]))
    return S_OK()

  def __checkoutFromHg(self, moduleName=None, sourceURL=None):
    """
    This method checkout a given tag from a hg repository.
    Note: we can checkout any project form a hg repository

    :param str moduleName: The name of the Module
    :param str sourceURL: The code repository

    """
    if not moduleName:
      moduleName = self.params.name

    if not sourceURL:
      sourceURL = self.params.sourceURL

    if self.params.vcsBranch:
      brCmr = "-b %s" % self.params.vcsBranch
    else:
      brCmr = ""
    fDirName = os.path.join(self.params.destination, moduleName)
    cmd = "hg clone %s '%s' '%s.tmp1'" % (brCmr,
                                          sourceURL,
                                          fDirName)
    gLogger.verbose("Executing: %s" % cmd)
    if os.system(cmd):
      return S_ERROR("Error while retrieving sources from hg")

    hgArgs = ["--cwd '%s.tmp1'" % fDirName]
    if self.params.vcsPath:
      hgArgs.append("--include '%s/*'" % self.params.vcsPath)
    hgArgs.append("'%s.tmp2'" % fDirName)

    cmd = "hg archive %s" % " ".join(hgArgs)
    gLogger.verbose("Executing: %s" % cmd)
    exportRes = os.system(cmd)
    shutil.rmtree("%s.tmp1" % fDirName)

    if exportRes:
      return S_ERROR("Error while exporting from hg")

    # TODO: tmp2/path to dest
    source = "%s.tmp2" % fDirName
    if self.params.vcsPath:
      source = os.path.join(source, self.params.vcsPath)

    if not os.path.isdir(source):
      shutil.rmtree("%s.tmp2" % fDirName)
      return S_ERROR("Path %s does not exist in repo")

    os.rename(source, fDirName)
    shutil.rmtree("%s.tmp2" % fDirName)

    return S_OK()

  @classmethod
  def replaceKeywordsWithGit(cls, dirToDo):
    for fileName in os.listdir(dirToDo):
      objPath = os.path.join(dirToDo, fileName)
      if os.path.isdir(objPath):
        TarModuleCreator.replaceKeywordsWithGit(objPath)
      elif os.path.isfile(objPath):
        if fileName.find('.py', len(fileName) - 3) == len(fileName) - 3:
          with open(objPath, "r") as fd:
            fileContents = fd.read()
          for keyWord, cmdArgs in (('$Id$', '--pretty="%h (%ad) %an <%aE>" --date=iso'),
                                   ('$SHA1$', '--pretty="%H"')):
            foundKeyWord = fileContents.find(keyWord)
            if foundKeyWord > -1:
              po2 = subprocess.Popen("git log -n 1 %s '%s' 2>/dev/null" % (cmdArgs, fileName),
                                     stdout=subprocess.PIPE, cwd=dirToDo, shell=True,
                                     universal_newlines=True)
              po2.wait()
              if po2.returncode:
                continue
              toReplace = po2.stdout.read().strip()
              toReplace = "".join(i for i in toReplace if ord(i) < 128)
              fileContents = fileContents.replace(keyWord, toReplace, 1)

          with open(objPath, "w") as fd:
            fd.write(fileContents)

  def __checkoutFromGit(self, moduleName=None, sourceURL=None, tagVersion=None):
    """
    This method checkout a given tag from a git repository.
    Note: we can checkout any project form a git repository

    :param str moduleName: The name of the Module: for example: LHCbWebDIRAC
    :param str sourceURL: The code repository: ssh://git@gitlab.cern.ch:7999/lhcb-dirac/LHCbWebDIRAC.git
    :param str tagVersion: the tag for example: v4r3p6

    """

    if not moduleName:
      moduleName = self.params.name

    if not sourceURL:
      sourceURL = self.params.sourceURL

    if not tagVersion:
      tagVersion = self.params.version

    if self.params.vcsBranch:
      brCmr = "-b %s" % self.params.vcsBranch
    else:
      brCmr = ""
    fDirName = os.path.join(self.params.destination, moduleName)
    cmd = "git clone %s '%s' '%s'" % (brCmr,
                                      sourceURL,
                                      fDirName)

    gLogger.verbose("Executing: %s" % cmd)
    if os.system(cmd):
      return S_ERROR("Error while retrieving sources from git")

    branchName = "DIRACDistribution-%s" % os.getpid()

    isTagCmd = "( cd '%s'; git tag -l | grep '%s' )" % (fDirName, tagVersion)
    if os.system(isTagCmd):
      # No tag found, assume branch
      branchSource = 'origin/%s' % tagVersion
    else:
      branchSource = tagVersion

    cmd = "( cd '%s'; git checkout -b '%s' '%s' )" % (fDirName, branchName, branchSource)

    gLogger.verbose("Executing: %s" % cmd)
    exportRes = os.system(cmd)

    # Add the keyword substitution
    gLogger.notice("Replacing keywords (can take a while)...")
    self.replaceKeywordsWithGit(fDirName)

    shutil.rmtree("%s/.git" % fDirName, ignore_errors=True)
    shutil.rmtree("%s/docs" % fDirName, ignore_errors=True)
    shutil.rmtree("%s/docs" % self.params.destination, ignore_errors=True)

    if exportRes:
      return S_ERROR("Error while exporting from git")

    return S_OK()

  def __loadReleaseNotesFile(self):
    if not self.params.relNotes:
      relNotes = os.path.join(self.params.destination, self.params.name, "release.notes")
    else:
      relNotes = self.params.relNotes
    if not os.path.isfile(relNotes):
      return S_OK("")
    try:
      with open(relNotes, "r") as fd:
        releaseContents = fd.readlines()
    except Exception as excp:
      return S_ERROR("Could not open %s: %s" % (relNotes, excp))
    gLogger.info("Loaded %s" % relNotes)
    relData = []
    version = False
    feature = False
    lastKey = False
    for rawLine in releaseContents:
      line = rawLine.strip()
      if not line:
        continue
      if line[0] == "[" and line[-1] == "]":
        version = line[1:-1].strip()
        relData.append((version, {'comment': [], 'features': []}))
        feature = False
        lastKey = False
        continue
      if line[0] == "*":
        feature = line[1:].strip()
        relData[-1][1]['features'].append([feature, {}])
        lastKey = False
        continue
      if not feature:
        relData[-1][1]['comment'].append(rawLine)
        continue
      keyDict = relData[-1][1]['features'][-1][1]
      foundKey = False
      for key in ('BUGFIX', 'BUG', 'FIX', "CHANGE", "NEW", "FEATURE"):
        if line.find("%s:" % key) == 0:
          line = line[len(key) + 2:].strip()
        elif line.find("%s " % key) == 0:
          line = line[len(key) + 1:].strip()
        else:
          continue
        foundKey = key
        break

      if foundKey in ('BUGFIX', 'BUG', 'FIX'):
        foundKey = 'BUGFIX'
      elif foundKey in ('NEW', 'FEATURE'):
        foundKey = 'FEATURE'

      if foundKey:
        if foundKey not in keyDict:
          keyDict[foundKey] = []
        keyDict[foundKey].append(line)
        lastKey = foundKey
      elif lastKey:
        keyDict[lastKey][-1] += " %s" % line

    return S_OK(relData)

  def __generateRSTFile(self, releaseData, rstFileName, pkgVersion, singleVersion):
    rstData = []
    parsedPkgVersion = Distribution.parseVersionString(pkgVersion)
    for version, verData in releaseData:
      if singleVersion and version != pkgVersion:
        continue
      if Distribution.parseVersionString(version) > parsedPkgVersion:
        continue
      versionLine = "Version %s" % version
      rstData.append("")
      rstData.append("=" * len(versionLine))
      rstData.append(versionLine)
      rstData.append("=" * len(versionLine))
      rstData.append("")
      if verData['comment']:
        rstData.append("\n".join(verData['comment']))
        rstData.append("")
      for feature, featureData in verData['features']:
        if not featureData:
          continue
        rstData.append(feature)
        rstData.append("=" * len(feature))
        rstData.append("")
        for key in sorted(featureData):
          rstData.append(key.capitalize())
          rstData.append(":" * (len(key) + 5))
          rstData.append("")
          for entry in featureData[key]:
            rstData.append(" - %s" % entry)
          rstData.append("")
    # Write releasenotes.rst
    try:
      rstFilePath = os.path.join(self.params.destination, self.params.name, rstFileName)
      with open(rstFilePath, "w") as fd:
        fd.write("\n".join(rstData))
    except Exception as excp:
      return S_ERROR("Could not write %s: %s" % (rstFileName, excp))
    return S_OK()

  def __generateReleaseNotes(self):
    result = self.__loadReleaseNotesFile()
    if not result['OK']:
      return result
    releaseData = result['Value']
    if not releaseData:
      gLogger.info("release.notes not found. Trying to find releasenotes.rst")
      for rstFileName in ("releasenotes.rst", "releasehistory.rst"):
        result = self.__compileReleaseNotes(rstFileName)
        if result['OK']:
          gLogger.notice("Compiled %s file!" % rstFileName)
        else:
          gLogger.warn(result['Message'])
      return S_OK()
    gLogger.info("Loaded release.notes")
    for rstFileName, singleVersion in (("releasenotes.rst", True),
                                       ("releasehistory.rst", False)):
      result = self.__generateRSTFile(releaseData, rstFileName, self.params.version,
                                      singleVersion)
      if not result['OK']:
        gLogger.error("Could not generate %s: %s" % (rstFileName, result['Message']))
        continue
      result = self.__compileReleaseNotes(rstFileName)
      if not result['OK']:
        gLogger.error("Could not compile %s: %s" % (rstFileName, result['Message']))
        continue
      gLogger.notice("Compiled %s file!" % rstFileName)
    return S_OK()

  def __compileReleaseNotes(self, rstFile):

    relNotesRST = os.path.join(self.params.destination, self.params.name, rstFile)
    if not os.path.isfile(relNotesRST):
      if self.params.relNotes:
        return S_ERROR("Defined release notes %s do not exist!" % self.params.relNotes)
      return S_ERROR("No release notes found in %s. Skipping" % relNotesRST)
    try:
      import docutils.core
    except ImportError:
      return S_ERROR("Docutils is not installed. Please install and rerun")
    # Find basename
    baseRSTFile = rstFile
    for ext in ('.rst', '.txt'):
      if baseRSTFile[-len(ext):] == ext:
        baseRSTFile = baseRSTFile[:-len(ext)]
        break
    baseNotesPath = os.path.join(self.params.destination, self.params.name, baseRSTFile)
    # To HTML
    try:
      with open(relNotesRST) as fd:
        rstData = fd.read()
    except Exception as excp:
      return S_ERROR("Could not read %s: %s" % (relNotesRST, excp))
    try:
      parts = docutils.core.publish_parts(rstData, writer_name='html')
    except Exception as excp:
      return S_ERROR("Cannot generate the html %s: %s" % (baseNotesPath, str(excp)))
    baseList = [baseNotesPath]
    if self.params.outRelNotes:
      gLogger.notice("Leaving a copy of the release notes outside the tarballs")
      baseList.append("%s/%s.%s.%s" % (self.params.destination, baseRSTFile, self.params.name, self.params.version))
    for baseFileName in baseList:
      htmlFileName = baseFileName + ".html"
      try:
        with open(htmlFileName, "w") as fd:
          fd.write(parts['whole'])
      except Exception as excp:
        return S_ERROR("Could not write %s: %s" % (htmlFileName, repr(excp).replace(',)', ')')))
    # Unlink if not necessary
    if False and not cliParams.relNotes:
      try:
        os.unlink(relNotesRST)
      except:
        pass
    return S_OK()

  def __generateTarball(self):
    destDir = self.params.destination
    tarName = "%s-%s.tar.gz" % (self.params.name, self.params.version)
    tarfilePath = os.path.join(destDir, tarName)
    dirToTar = os.path.join(self.params.destination, self.params.name)
    if self.params.name in os.listdir(dirToTar):
      dirToTar = os.path.join(dirToTar, self.params.name)
    result = Distribution.writeVersionToInit(dirToTar, self.params.version)
    if not result['OK']:
      return result
    result = Distribution.createTarball(tarfilePath, dirToTar)
    if not result['OK']:
      return S_ERROR("Could not generate tarball: %s" % result['Error'])
    # Remove package dir
    shutil.rmtree(dirToTar)
    gLogger.info("Tar file %s created" % tarName)
    return S_OK(tarfilePath)

  def __compileWebApp(self):
    """
    This method is compile the DIRAC web framework
    """
    dctArgs = []
    if self.params.extjspath:
      dctArgs.append("-P '%s'" % self.params.extjspath)

    destDir = self.params.destination
    dctArgs.append("-D '%s'" % destDir)
    scriptName = os.path.join("%s/WebAppDIRAC/scripts/" % destDir, "dirac-webapp-compile.py")
    if not os.path.isfile(scriptName):
      return S_ERROR("%s file does not exists!" % scriptName)
    dctArgs.append("-n '%s'" % self.params.name)
    cmd = "'%s' %s" % (scriptName, " ".join(dctArgs))
    gLogger.verbose("Executing %s" % cmd)
    if os.system(cmd) != 0:
      return S_ERROR("Failed to execute the command")
    return S_OK()

  def create(self):
    if not isinstance(self.params, TarModuleCreator.Params):
      return S_ERROR("Argument is not a TarModuleCreator.Params object ")
    result = self.params.isOK()
    if not result['OK']:
      return result
    result = self.__checkDestination()
    if not result['OK']:
      return result
    result = self.__checkoutSource()
    if not result['OK']:
      return result
    shutil.rmtree("%s/docs" % self.params.destination, ignore_errors=True)
    result = self.__generateReleaseNotes()
    if not result['OK']:
      gLogger.error("Won't generate release notes: %s" % result['Message'])

    if 'Web' in self.params.name and self.params.name != 'Web':
      # if we have an extension, we have to download, because it will be
      # required to compile the code
      if self.params.extensionVersion and self.params.extensionSource:
        # if extensionSource is not provided, the default one is used. self.params.soureURL....
        result = self.__checkoutSource("WebAppDIRAC", self.params.extensionSource, self.params.extensionVersion)
        if not result['OK']:
          return result
      retVal = self.__compileWebApp()
      if not retVal['OK']:  # it can fail, if we do not have sencha cmd and extjs farmework installed
        gLogger.warn('Web is not compiled: %s' % retVal['Message'])

    return self.__generateTarball()


if __name__ == "__main__":
  cliParams = TarModuleCreator.Params()

  Script.disableCS()
  Script.addDefaultOptionValue("/DIRAC/Setup", "Dummy")
  Script.registerSwitch("v:", "version=", "version to tar", cliParams.setVersion)
  Script.registerSwitch("u:", "source=", "VCS path to retrieve sources from", cliParams.setSourceURL)
  Script.registerSwitch("D:", "destination=", "Destination where to build the tar files", cliParams.setDestination)
  Script.registerSwitch("n:", "name=", "Tarball name", cliParams.setName)
  Script.registerSwitch(
      "z:", "vcs=", "VCS to use to retrieve the sources (try to find out if not specified)", cliParams.setVCS)
  Script.registerSwitch("b:", "branch=", "VCS branch (if needed)", cliParams.setVCSBranch)
  Script.registerSwitch("p:", "path=", "VCS path (if needed)", cliParams.setVCSPath)
  Script.registerSwitch("K:", "releasenotes=", "Path to the release notes", cliParams.setReleaseNotes)
  Script.registerSwitch(
      "A", "notesoutside", "Leave a copy of the compiled release notes outside the tarball",
      cliParams.setOutReleaseNotes)
  Script.registerSwitch("e:", "extensionVersion=", "if we have an extension, "
                        "we can provide the base module version "
                        "(if it is needed): for example: v3r0", cliParams.setExtensionVersion)
  Script.registerSwitch("E:", "extensionSource=", "if we have an extension "
                              "we must provide code repository url", cliParams.setExtensionSource)
  Script.registerSwitch("P:", "extjspath=", "directory of the extjs library", cliParams.setExtJsPath)

  Script.setUsageMessage('\n'.join([__doc__.split('\n')[1],
                                    '\nUsage:',
                                    '  %s <option> ...\n' % Script.scriptName,
                                    '  A source, name and version are required to build the tarball',
                                    '  For instance:',
                                    '     %s -n DIRAC -v v1r0 -z '
                                    'svn -u http://svnweb.cern.ch/guest/'
                                    'dirac/DIRAC/tags/DIRAC/v1r0' % Script.scriptName]))

  Script.parseCommandLine(ignoreErrors=False)

  result = cliParams.isOK()
  if not result['OK']:
    gLogger.error(result['Message'])
    Script.showHelp()
    sys.exit(1)

  tmc = TarModuleCreator(cliParams)
  result = tmc.create()
  if not result['OK']:
    gLogger.error("Could not create the tarball: %s" % result['Message'])
    sys.exit(1)
  gLogger.always("Tarball successfully created at %s" % result['Value'])
  sys.exit(0)
