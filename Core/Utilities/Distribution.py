""" Utilities for distributing DIRAC
"""

__RCSID__ = "$Id$"

import re
import tarfile
import os

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities import File


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

#Start of release notes

gAllowedNoteTypes = ( "NEW", "CHANGE", "BUGFIX", 'FIX' )
gNoteTypeAlias = { 'FIX' : 'BUGFIX' }

def retrieveReleaseNotes( packages ):
  if isinstance( packages, basestring ):
    packages = [ str( packages ) ]
  packageCFGDict = {}
  #Get the versions.cfg
  for package in packages:
    packageCFGDict[ package ] = Distribution( package ).getVersionsCFG()
  #Parse the release notes
  pkgNotesDict = {}
  for package in packageCFGDict:
    versionsCFG = packageCFGDict[ package ][ 'Versions' ]
    pkgNotesDict[ package ] = []
    for mainVersion in versionsCFG.listSections( ordered = True ):
      vCFG = versionsCFG[ mainVersion ]
      versionNotes = {}
      for subsys in vCFG.listOptions():
        comment = vCFG.getComment( subsys )
        if not comment:
          continue
        versionNotes[ subsys ] = {}
        lines = List.fromChar( comment, "\n" )
        lastCommentType = False
        for line in lines:
          processedLine = False
          for typeComment in gAllowedNoteTypes:
            if line.find( "%s:" % typeComment ) == 0:
              if typeComment in gNoteTypeAlias:
                effectiveType = gNoteTypeAlias[ typeComment ]
              else:
                effectiveType = typeComment
              if effectiveType not in versionNotes[ subsys ]:
                versionNotes[ subsys ][ effectiveType ] = []
              versionNotes[ subsys ][ effectiveType ].append( line[ len( typeComment ) + 1: ].strip() )
              lastCommentType = effectiveType
              processedLine = True
          if not processedLine and lastCommentType:
            versionNotes[ subsys ][ effectiveType ][-1] += " %s" % line.strip()
      if versionNotes:
        pkgNotesDict[ package ].append( { 'version' : mainVersion, 'notes' : versionNotes } )
      versionComment = versionsCFG.getComment( mainVersion )
      if versionComment:
        pkgNotesDict[ package ][-1][ 'comment' ] = "\n".join( [ l.strip() for l in versionComment.split( "\n" ) ] )
  return pkgNotesDict

def generateReleaseNotes( packages, destinationPath, versionReleased = "", singleVersion = False ):
  if isinstance( packages, basestring ):
    packages = [ str( packages ) ]
  pkgNotesDict = retrieveReleaseNotes( packages )
  fileContents = []
  foundStartVersion = versionReleased == ""
  for package in packages:
    if package not in pkgNotesDict:
      continue
    #Add a section with the package name
    dummy = "Package %s" % package
    fileContents.append( "-" * len( dummy ) )
    fileContents.append( dummy )
    fileContents.append( "-" * len( dummy ) )
    vNotesDict = pkgNotesDict[ package ]
    for versionNotes in vNotesDict:
      if singleVersion and versionReleased and versionNotes[ 'version' ] != versionReleased:
        continue
      if versionReleased and versionReleased == versionNotes[ 'version' ]:
        foundStartVersion = True
      #Skip until found initial version
      if not foundStartVersion:
        continue
      dummy = "Version %s" % versionNotes[ 'version' ]
      fileContents.append( "" )
      fileContents.append( dummy )
      fileContents.append( "-" * len( dummy ) )
      if 'comment' in versionNotes:
        fileContents.extend( [ '', versionNotes[ 'comment' ], '' ] )
      for noteType in gAllowedNoteTypes:
        notes4Type = []
        for system in versionNotes[ 'notes' ]:
          if noteType in versionNotes[ 'notes' ][ system ] and versionNotes[ 'notes' ][ system ][ noteType ]:
            notes4Type.append( " %s" % system )
            for line in versionNotes[ 'notes' ][ system ][ noteType ]:
              notes4Type.append( "  - %s" % line )
        if notes4Type:
          fileContents.append( "" )
          fileContents.append( "%s" % noteType )
          fileContents.append( ":" * len( noteType ) )
          fileContents.append( "" )
          fileContents.extend( notes4Type )
  fd = open( destinationPath, "w" )
  fd.write( "%s\n\n" % "\n".join( fileContents ) )
  fd.close()

def generateHTMLReleaseNotesFromRST( rstFile, htmlFile ):
  try:
    import docutils.core
  except ImportError:
    gLogger.error( "Docutils is not installed, skipping generation of release notes in html format" )
    return False
  try:
    fd = open( rstFile )
    rstData = fd.read()
    fd.close()
  except Exception:
    gLogger.error( "Oops! Could not read the rst file :P" )
    return False
  parts = docutils.core.publish_parts( rstData, writer_name = 'html' )
  try:
    fd = open( htmlFile, "w" )
    fd.write( parts[ 'whole' ] )
    fd.close()
  except Exception:
    gLogger.error( "Oops! Could not write the html file :P" )
    return False
  return True
