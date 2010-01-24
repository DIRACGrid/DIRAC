# $HeadURL$
__RCSID__ = "$Id$"

import urllib2, re, tarfile, os, types, sys, subprocess

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities import CFG, File, List

def execAndGetOutput( cmd ):
  p = subprocess.Popen( cmd,
                        shell = True, stdout = subprocess.PIPE,
                        stderr = subprocess.PIPE, close_fds = True )
  stdData = p.stdout.read()
  p.wait()
  return ( p.returncode, stdData )

def getRepositoryVersions( package = False ):
  if package:
    webLocation = 'http://svnweb.cern.ch/guest/dirac/%s/tags/%s' % ( package, package )
  else:
    webLocation = 'http://svnweb.cern.ch/guest/dirac/tags'
    package = "global release"
  try:
    remoteFile = urllib2.urlopen( webLocation )
  except urllib2.URLError:
    gLogger.exception()
    sys.exit( 2 )
  remoteData = remoteFile.read()
  remoteFile.close()
  if not remoteData:
    gLogger.error( "Could not retrieve versions for package %s" % package )
    sys.exit( 1 )
  versions = []
  rePackage = ".*"
  versionRE = re.compile( "<li> *<a *href=.*> *(%s)/ *</a> *</li>" % rePackage )
  for line in remoteData.split( "\n" ):
    res = versionRE.search( line )
    if res:
      versions.append( res.groups()[0] )
  return versions

def getSVNFileContents( svnPath ):
  import urllib2, stat
  gLogger.info( "Reading %s" % ( svnPath ) )
  viewSVNLocation = "http://svnweb.cern.ch/world/wsvn/dirac/%s?op=dl&rev=0" % ( svnPath )
  anonymousLocation = 'http://svnweb.cern.ch/guest/dirac/%s' % ( svnPath )
  for remoteLocation in ( viewSVNLocation, anonymousLocation ):
    try:
      remoteFile = urllib2.urlopen( remoteLocation )
    except urllib2.URLError:
      gLogger.exception()
      continue
    remoteData = remoteFile.read()
    remoteFile.close()
    if remoteData:
      return remoteData
  #Web cat failed. Try directly with svn
  exitStatus, remoteData = execAndGetOutput( "svn cat 'http://svnweb.cern.ch/guest/dirac/%s" % ( svnPath ) )
  if exitStatus:
    print "Error: Could not retrieve %s from the web nor via SVN. Aborting..." % svnPath
    sys.exit( 1 )
  return remoteData

def loadCFGFromRepository( svnPath ):
  remoteData = getSVNFileContents( svnPath )
  return CFG.CFG().loadFromBuffer( remoteData )

def createTarball( tarballPath, directoryToTar, additionalDirectoriesToTar = [] ):
  tf = tarfile.open( tarballPath, "w:gz" )
  tf.add( directoryToTar, os.path.basename( os.path.abspath( directoryToTar ) ), recursive = True )
  if type( additionalDirectoriesToTar ) in ( types.StringType, types.UnicodeType ):
    additionalDirectoriesToTar = [ additionalDirectoriesToTar ]
  for dirToTar in additionalDirectoriesToTar:
    if os.path.isdir( dirToTar ):
      tf.add( dirToTar, os.path.basename( os.path.abspath( dirToTar ) ), recursive = True )
  tf.close()
  md5FilePath = False
  for suffix in ( ".tar.gz", ".gz" ):
    sLen = len( suffix )
    if tarballPath[ len( tarballPath ) - sLen: ] == suffix:
      md5FilePath = "%s.md5" % tarballPath[:-sLen]
      break
  if not md5FilePath:
    return S_ERROR( "Could not generate md5 filename" )
  md5str = File.getMD5ForFiles( [ tarballPath ] )
  fd = open( md5FilePath, "w" )
  fd.write( md5str )
  fd.close()
  return S_OK()

def retrieveReleaseNotes( packages ):
  if type( packages ) in ( types.StringType, types.UnicodeType ):
    packages = [ str( packages ) ]
  packageCFGDict = {}
  #Get the versions.cfg
  for package in packages:
    packageCFGDict[ package ] = loadCFGFromRepository( "%s/trunk/%s/versions.cfg" % ( package, package ) )
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
        for typeComment in ( "NEW", "CHANGE", "BUGFIX" ):
          for line in lines:
            if line.find( "%s:" % typeComment ) == 0:
              if typeComment not in versionNotes[ subsys ]:
                versionNotes[ subsys ][ typeComment ] = []
              versionNotes[ subsys ][ typeComment ].append( line[ len( typeComment ) + 1: ].strip() )
      if versionNotes:
        pkgNotesDict[ package ].append( { 'version' : mainVersion, 'notes' : versionNotes } )
      versionComment = versionsCFG.getComment( mainVersion )
      if versionComment:
        pkgNotesDict[ package ][-1][ 'comment' ] = "\n".join( [ l.strip() for l in versionComment.split( "\n" ) ] )
  return pkgNotesDict

def generateReleaseNotes( packages, destinationPath, versionReleased = "", singleVersion = False ):
  if type( packages ) in ( types.StringType, types.UnicodeType ):
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
      for noteType in ( "NEW", "CHANGE", "BUGFIX" ):
        notes4Type = []
        for system in versionNotes[ 'notes' ]:
          if noteType in versionNotes[ 'notes' ][ system ] and versionNotes[ 'notes' ][ system ][ noteType ]:
            notes4Type.append( " %s" % system )
            for line in versionNotes[ 'notes' ][ system ][ noteType ]:
              notes4Type.append( "  - %s" % line )
        if notes4Type:
          fileContents.append( ":%s:" % noteType )
          fileContents.extend( notes4Type )
  fd = open( destinationPath, "w" )
  fd.write( "%s\n\n" % "\n".join( fileContents ) )
  fd.close()

def generateHTMLReleaseNotesFromRST( rstFile, htmlFile ):
  try:
    import docutils.core
  except:
    gLogger.error( "Docutils is not installed, skipping generation of release notes in html format" )
    return False
  try:
    fd = open( rstFile )
    rstData = fd.read()
    fd.close()
  except:
    gLogger.error( "Oops! Could not read the rst file :P" )
    return False
  parts = docutils.core.publish_parts( rstData, writer_name = 'html' )
  try:
    fd = open( htmlFile, "w" )
    fd.write( parts[ 'whole' ] )
    fd.close()
  except:
    gLogger.error( "Oops! Could not write the html file :P" )
    return False
  return True
