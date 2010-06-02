# $HeadURL$
__RCSID__ = "$Id$"

import urllib2, re, tarfile, os, types, sys, subprocess, urlparse

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities import CFG, File, List

class Distribution:

  cernAnonRoot = 'http://svnweb.cern.ch/guest/dirac'
  googleAnonRoot = 'http://dirac-grid.googlecode.com/svn'

  cernDevRoot = 'svn+ssh://svn.cern.ch/reps/dirac'
  googleDevRoot = 'https://dirac-grid.googlecode.com/svn'

  anonymousSVNRoot = { 'global' : cernAnonRoot,
                       'DIRAC' : cernAnonRoot,
                       'LHCbDIRAC' : cernAnonRoot,
                       'BelleDIRAC' : googleAnonRoot,
                       'EELADIRAC' : googleAnonRoot
                     }

  devSVNRoot = { 'global' : cernDevRoot,
                 'DIRAC' : cernDevRoot,
                 'LHCbDIRAC' : cernDevRoot,
                 'BelleDIRAC' : googleDevRoot,
                 'EELADIRAC' : googleDevRoot
               }

  def __init__( self, package = False ):
    if not package:
      package = 'global'
    if package not in Distribution.anonymousSVNRoot:
      raise Exception( "Package %s does not have a registered svn root" % package )
    self.package = package
    self.svnRoot = Distribution.anonymousSVNRoot[ package ]
    self.svnPass = False
    self.svnUser = False
    self.cmdQueue = []

  def getSVNPathForPackage( self, package, path ):
    if package not in self.anonymousSVNRoot:
      return "%s/%s" % ( Distribution.cernAnonRoot, path )
    return "%s/%s" % ( self.anonymousSVNRoot[ package ], path )

  def getPackageName( self ):
    return self.package

  def getDevPath( self, path = False ):
    devPath = Distribution.devSVNRoot[ self.package ]
    if path:
      devPath += "/%s" % path
    return devPath

  def setSVNPassword( self, password ):
    self.svnPass = password

  def setSVNUser( self, user ):
    self.svnUser = user

  def addCommandToQueue( self, cmd ):
    self.cmdQueue.append( cmd )

  def executeCommandQueue( self ):
    while self.cmdQueue:
      if not self.executeCommand( self.cmdQueue.pop( 0 ), getOutput = False ):
        return False
    return True

  def emptyQueue( self ):
    return len( self.cmdQueue ) == 0

  def getRepositoryVersions( self ):
    if self.package == 'global' :
      webLocation = "%s/tags" % self.svnRoot
    else:
      webLocation = '%s/%s/tags/%s' % ( self.svnRoot, package, package )
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

  def getSVNFileContents( self, svnPath ):
    import urllib2, stat
    gLogger.info( "Reading %s" % ( svnPath ) )
    remoteLocation = "%s/%s" % ( self.svnRoot, svnPath )
    try:
      remoteFile = urllib2.urlopen( remoteLocation )
    finally:
      remoteData = remoteFile.read()
      remoteFile.close()
      if remoteData:
        return remoteData
    #Web cat failed. Try directly with svn
    exitStatus, remoteData = execAndGetOutput( "svn cat '%s" % remoteLocation )
    if exitStatus:
      print "Error: Could not retrieve %s from the web nor via SVN. Aborting..." % svnPath
      sys.exit( 1 )
    return remoteData

  def loadCFGFromRepository( self, svnPath ):
    remoteData = self.getSVNFileContents( svnPath )
    return CFG.CFG().loadFromBuffer( remoteData )

  def getVersionsCFG( self ):
    return self.loadCFGFromRepository( '%s/trunk/%s/versions.cfg' % ( self.package, self.package ) )

  def executeCommand( self, cmd, getOutput = True ):
    env = dict( os.environ )
    if self.svnPass:
      env[ 'SVN_PASSWORD' ] = self.svnPass
    if not getOutput:
      return subprocess.Popen( cmd, shell = True, env = env ).wait() == 0
    #Get output
    p = subprocess.Popen( cmd,
                          shell = True, stdout = subprocess.PIPE,
                          stderr = subprocess.PIPE, close_fds = True, env = env )
    stdData = p.stdout.read()
    p.wait()
    return ( p.returncode, stdData )

  def __getDevCmdBase( self, path ):
    devRoot = self.getDevPath( path )
    isHTTPS = False
    urlRes = urlparse.urlparse( devRoot )
    args = []
    if urlRes.scheme == "https":
      isHTTPS = True

    if self.svnUser:
      if isHTTPS:
        args.append( "--username '%s'" % self.svnUser )
      else:
        urlRes = urlparse.urlparse( devRoot )
        devRoot = urlparse.urlunparse( ( urlRes.scheme,
                                         "%s@%s" % ( self.svnUser, urlRes.netloc ),
                                         urlRes.path,
                                         urlRes.params,
                                         urlRes.query, urlRes.fragment ) )

    if self.svnPass and isHTTPS:
      args.append( "--password '%s'" % self.svnPass )

    return ( " ".join( args ), devRoot )


  def doLS( self, path ):
    t = self.__getDevCmdBase( path )
    cmd = "svn ls %s %s" % t
    return self.executeCommand( cmd, True )

  def __cmdImport( self, origin, dest, comment ):
    destT = self.__getDevCmdBase( dest )
    cmd = "svn import -m '%s' %s %s %s" % ( comment, destT[0], origin, destT[1] )
    return cmd

  def queueImport( self, origin, dest, comment ):
    self.addCommandToQueue( self.__cmdImport( origin, dest, comment ) )

  def doImport( self, origin, dest, comment ):
    return self.executeCommand( self.__cmdImport( origin, dest, comment ), False )

  def __cmdCopy( self, origin, dest, comment ):
    destT = self.__getDevCmdBase( dest )
    orT = self.__getDevCmdBase( origin )
    cmd = "svn copy -m '%s' %s %s %s" % ( comment, destT[0], orT[1], destT[1] )
    return cmd

  def queueCopy( self, origin, dest, comment ):
    self.addCommandToQueue( self.__cmdCopy( origin, dest, comment ) )

  def doCopy( self, path, comment ):
    return self.executeCommand( self.__cmdCopy( origin, dest, comment ), False )

  def __cmdMakeDir( self, path, comment ):
    t = self.__getDevCmdBase( path )
    return "svn mkdir -m '%s' %s %s" % ( comment, t[0], t[1] )

  def queueMakeDir( self, path, comment ):
    self.addCommandToQueue( self.__cmdMakeDir( path, comment ) )

  def doMakeDir( self, path, comment ):
    return self.executeCommand( self.__cmdMakeDir( path, comment ), False )

  def doCheckout( self, path, location ):
    t = self.__getDevCmdBase( path )
    cmd = "svn co %s '%s' '%s'" % ( t[0], t[1], location )
    return self.executeCommand( cmd, False )

  def doCommit( self, location, comment ):
    t = self.__getDevCmdBase( "" )
    cmd = "svn ci -m '%s' %s '%s'" % ( comment, t[0], location )
    return self.executeCommand( cmd, False )





#End of Distribution class



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

#Start of release notes

allowedNoteTypes = ( "NEW", "CHANGE", "BUGFIX", 'FIX' )
noteTypeAlias = { 'FIX' : 'BUGFIX' }

def retrieveReleaseNotes( packages ):
  if type( packages ) in ( types.StringType, types.UnicodeType ):
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
          for typeComment in allowedNoteTypes:
            if line.find( "%s:" % typeComment ) == 0:
              if typeComment in noteTypeAlias:
                effectiveType = noteTypeAlias[ typeComment ]
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
      for noteType in allowedNoteTypes:
        notes4Type = []
        for system in versionNotes[ 'notes' ]:
          if noteType in versionNotes[ 'notes' ][ system ] and versionNotes[ 'notes' ][ system ][ noteType ]:
            notes4Type.append( " %s" % system )
            for line in versionNotes[ 'notes' ][ system ][ noteType ]:
              notes4Type.append( "  - %s" % line )
        if notes4Type:
          fileContents.append("")
          fileContents.append( "%s" % noteType )
          fileContents.append( ":" * len( noteType ) )
          fileContents.append("")
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
