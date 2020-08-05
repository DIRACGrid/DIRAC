""" Utilities for distributing DIRAC
"""

__RCSID__ = "$Id$"

import re
import tarfile
import os

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities import File, List


gVersionRE = re.compile("v([0-9]+)(?:r([0-9]+))?(?:p([0-9]+))?(?:-pre([0-9]+))?")


def parseVersionString(version):
  result = gVersionRE.match(version.strip())
  if not result:
    return False
  vN = []
  for e in result.groups():
    if e:
      vN.append(int(e))
    else:
      vN.append(None)
  return tuple(vN)


def writeVersionToInit(rootPath, version):
  verTup = parseVersionString(version)
  if not verTup:
    return S_OK()
  initFile = os.path.join(rootPath, "__init__.py")
  if not os.path.isfile(initFile):
    return S_OK()
  try:
    with open(initFile, "r") as fd:
      fileData = fd.read()
  except Exception as e:
    return S_ERROR("Could not open %s: %s" % (initFile, str(e)))
  versionStrings = ("majorVersion", "minorVersion", "patchLevel", "preVersion")
  reList = []
  for iP, version in enumerate(versionStrings):
    if verTup[iP]:
      replStr = "%s = %s" % (versionStrings[iP], verTup[iP])
    else:
      replStr = "%s = 0" % versionStrings[iP]
    reList.append((re.compile(r"^(%s\s*=)\s*[0-9]+\s*" % versionStrings[iP]), replStr))
  newData = []
  for line in fileData.split("\n"):
    for reCm, replStr in reList:
      line = reCm.sub(replStr, line)
    newData.append(line)
  try:
    with open(initFile, "w") as fd:
      fd.write("\n".join(newData))
  except Exception as e:
    return S_ERROR("Could write to %s: %s" % (initFile, str(e)))
  return S_OK()


def createTarball(tarballPath, directoryToTar, additionalDirectoriesToTar=None):
  tf = tarfile.open(tarballPath, "w:gz")
  tf.add(directoryToTar, os.path.basename(os.path.abspath(directoryToTar)), recursive=True)
  if isinstance(additionalDirectoriesToTar, basestring):
    additionalDirectoriesToTar = [additionalDirectoriesToTar]
  if additionalDirectoriesToTar:
    for dirToTar in additionalDirectoriesToTar:
      if os.path.isdir(dirToTar):
        tf.add(dirToTar, os.path.basename(os.path.abspath(dirToTar)), recursive=True)
  tf.close()
  md5FilePath = False
  for suffix in (".tar.gz", ".gz"):
    sLen = len(suffix)
    if tarballPath.endswith(suffix):
      md5FilePath = "%s.md5" % tarballPath[:-sLen]
      break
  if not md5FilePath:
    return S_ERROR("Could not generate md5 filename")
  md5str = File.getMD5ForFiles([tarballPath])
  fd = open(md5FilePath, "w")
  fd.write(md5str)
  fd.close()
  return S_OK()
