#!/usr/bin/env python
# -*- coding: utf-8 -*-
########################################################################
# $HeadURL$
# File :    dirac-distribution-create-tarball
# Author :  Adria Casajus
########################################################################
"""
  Create tarballs for a given DIRAC release
"""
__RCSID__ = "$Id$"

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Base      import Script
from DIRAC.Core.Utilities import List, File, Distribution, Platform, Subprocess

import sys, os, shutil, tempfile, getpass

class TarModuleCreator( object ):

  VALID_VCS = ( 'cvs', 'svn', 'git', 'hg', 'file' )

  class Params( object ):

    def __init__( self ):
      self.version = False
      self.destination = False
      self.sourceURL = False
      self.name = False
      self.vcs = False
      self.vcsBranch = False
      self.vcsPath = False
      self.relNotes = False
      self.outRelNotes = False

    def isOK( self ):
      if not self.version:
        return S_ERROR( "No version defined" )
      if not self.sourceURL:
        return S_ERROR( "No Source URL defined" )
      if not self.name:
        return S_ERROR( "No name defined" )
      if self.vcs and self.vcs not in TarModuleCreator.VALID_VCS:
        return S_ERROR( "Invalid VCS %s" % self.vcs )
      return S_OK()

    def setVersion( self, opVal ):
      self.version = opVal
      return S_OK()

    def setDestination( self, opVal ):
      self.destination = os.path.realpath( opVal )
      return S_OK()

    def setSourceURL( self, opVal ):
      self.sourceURL = opVal
      return S_OK()

    def setName( self, opVal ):
      self.name = opVal
      return S_OK()

    def setVCS( self, opVal ):
      self.vcs = opVal.lower()
      if self.vcs == 'subversion':
        self.vcs = 'svn'
      elif self.vcs == 'mercurial':
        self.vcs = 'hg'
      return S_OK()

    def setVCSBranch( self, opVal ):
      self.vcsBranch = opVal
      return S_OK()

    def setVCSPath( self, opVal ):
      self.vcsPath = opVal
      return S_OK()

    def setReleaseNotes( self, opVal ):
      self.relNotes = opVal
      return S_OK()
    
    def setOutReleaseNotes(self, opVal ):
      self.outRelNotes = True
      return S_OK()

  def __init__( self, params ):
    self.params = params

  def __checkDestination( self ):
    if not self.params.destination:
      self.params.destination = tempfile.mkdtemp( 'DIRACTarball' )

    gLogger.notice( "Will generate tarball in %s" % self.params.destination )

    if not os.path.isdir( self.params.destination ):
      try:
        os.makedirs( self.params.destination )
      except Exception, e:
        return S_ERROR( "Cannot write to destination: %s" % str( e ) )

    return S_OK()

  def __discoverVCS( self ):
    sourceURL = self.params.sourceURL
    if os.path.expanduser( sourceURL ).find( "/" ) == 0:
      sourceURL = os.path.expanduser( sourceURL )
      self.params.vcs = "file"
      return True
    if sourceURL.find( ":" ) == 0:
      self.params.vcs = "cvs"
      return True
    if sourceURL.find( ".git" ) == len( sourceURL ) - 4:
      self.params.vcs = "git"
      return True
    for vcs in TarModuleCreator.VALID_VCS:
      if sourceURL.find( vcs ) == 0:
        self.params.vcs = vcs
        return True
    return False

  def __checkoutSource( self ):
    if not self.params.vcs:
      if not self.__discoverVCS():
        return S_ERROR( "Could not autodiscover VCS" )
    gLogger.info( "Checking out using %s method" % self.params.vcs )

    if self.params.vcs == "file":
      return self.__checkoutFromFile()
    elif self.params.vcs == "cvs":
      return self.__checkoutFromCVS()
    elif self.params.vcs == "svn":
      return self.__checkoutFromSVN()
    elif self.params.vcs == "hg":
      return self.__checkoutFromHg()
    elif self.params.vcs == "git":
      return self.__checkoutFromGit()

    return S_ERROR( "OOPS. Unknown VCS %s!" % self.params.vcs )

  def __checkoutFromFile( self ):
    sourceURL = self.params.sourceURL
    if sourceURL.find( "file://" ) == 0:
      sourceURL = sourceURL[ 7: ]
    sourceURL = os.path.realpath( sourceURL )
    try:
      pyVer = sys.version_info
      if pyVer[0] == 2 and pyVer[1] < 6:
        shutil.copytree( sourceURL,
                         os.path.join( self.params.destination, self.params.name ),
                         symlinks = True )
      else:
        shutil.copytree( sourceURL,
                         os.path.join( self.params.destination, self.params.name ),
                         symlinks = True,
                         ignore = shutil.ignore_patterns( '.svn', '.git', '.hg', '*.pyc', '*.pyo', 'CVS' ) )
    except Exception, e:
      return S_ERROR( "Could not copy data from source URL: %s" % str( e ) )
    return S_OK()

  def __checkoutFromCVS( self ):
    cmd = "cvs export -d '%s' '%s'" % ( self.params.sourceURL, os.path.join( self.params.destination, self.params.name ) )
    gLogger.verbose( "Executing: %s" % cmd )
    result = Subprocess.shellCall( 900, cmd )
    if not result[ 'OK' ]:
      return S_ERROR( "Error while retrieving sources from CVS: %s" % result[ 'Message' ] )
    exitStatus, stdData, errData = result[ 'Value' ]
    if exitStatus:
      return S_ERROR( "Error while retrieving sources from CVS: %s" % "\n".join( [ stdData, errData ] ) )
    return S_OK()

  def __checkoutFromSVN( self ):
    cmd = "svn export --trust-server-cert --non-interactive '%s/%s' '%s'" % ( self.params.sourceURL, self.params.version,
                                                                              os.path.join( self.params.destination, self.params.name ) )
    gLogger.verbose( "Executing: %s" % cmd )
    result = Subprocess.shellCall( 900, cmd )
    if not result[ 'OK' ]:
      return S_ERROR( "Error while retrieving sources from SVN: %s" % result[ 'Message' ] )
    exitStatus, stdData, errData = result[ 'Value' ]
    if exitStatus:
      return S_ERROR( "Error while retrieving sources from SVN: %s" % "\n".join( [ stdData, errData ] ) )
    return S_OK()

  def __checkoutFromHg( self ):
    if self.params.vcsBranch:
      brCmr = "-b %s" % self.params.vcsBranch
    else:
      brCmr = ""
    fDirName = os.path.join( self.params.destination, self.params.name )
    cmd = "hg clone %s '%s' '%s.tmp1'" % ( brCmr,
                                      self.params.sourceURL,
                                      fDirName )
    gLogger.verbose( "Executing: %s" % cmd )
    if os.system( cmd ):
      return S_ERROR( "Error while retrieving sources from hg" )

    hgArgs = [ "--cwd '%s.tmp1'" % fDirName ]
    if self.params.vcsPath:
      hgArgs.append( "--include '%s/*'" % self.params.vcsPath )
    hgArgs.append( "'%s.tmp2'" % fDirName )

    cmd = "hg archive %s" % " ".join( hgArgs )
    gLogger.verbose( "Executing: %s" % cmd )
    exportRes = os.system( cmd )
    shutil.rmtree( "%s.tmp1" % fDirName )

    if exportRes:
      return S_ERROR( "Error while exporting from hg" )

    #TODO: tmp2/path to dest
    source = "%s.tmp2" % fDirName
    if self.params.vcsPath:
      source = os.path.join( source, self.params.vcsPath )

    if not os.path.isdir( source ):
      shutil.rmtree( "%s.tmp2" % fDirName )
      return S_ERROR( "Path %s does not exist in repo" )

    os.rename( source, fDirName )
    shutil.rmtree( "%s.tmp2" % fDirName )

    return S_OK()

  def __checkoutFromGit( self ):
    if self.params.vcsBranch: 
      brCmr = "-b %s" % self.params.vcsBranch
    else:
      brCmr = ""
    fDirName = os.path.join( self.params.destination, self.params.name )
    cmd = "git clone %s '%s' '%s'" % ( brCmr,
                                           self.params.sourceURL,
                                           fDirName )
    gLogger.verbose( "Executing: %s" % cmd )
    if os.system( cmd ):
      return S_ERROR( "Error while retrieving sources from git" )

    branchName = "DIRACDistribution-%s" % os.getpid()

    cmd = "( cd '%s'; git checkout -b '%s' '%s' )" % ( fDirName, branchName, self.params.version )

    gLogger.verbose( "Executing: %s" % cmd )
    exportRes = os.system( cmd )
    shutil.rmtree( "%s/.git" % fDirName )

    if exportRes:
      return S_ERROR( "Error while exporting from git" )

    return S_OK()


  def __loadReleaseNotesFile( self ):
    if not self.params.relNotes:
      relNotes = os.path.join( self.params.destination, self.params.name, "release.notes" )
    else:
      relNotes = self.params.relNotes
    if not os.path.isfile( relNotes ):
      return S_OK( "" )
    try:
      fd = open( relNotes, "r" )
      relaseContents = fd.readlines()
      fd.close()
    except Exception, excp:
      return S_ERROR( "Could not open %s: %s" % ( relNotes, excp ) )
    gLogger.info( "Loaded %s" % relNotes )
    relData = []
    version = False
    feature = False
    for rawLine in relaseContents:
      line = rawLine.strip()
      if not line:
        continue
      if version == False:
        if line[0] == "[" and line[-1] == "]":
          version = line[1:-1].strip()
          relData.append( ( version, { 'comment' : [], 'features' : [] } ) )
          feature = False
        continue
      if line[0] == "*":
        feature = line[1:].strip()
        relData[-1][1][ 'features' ].append( [ feature, {} ] )
        continue
      if not feature:
        relData[ -1 ][1][ 'comment' ].append( rawLine )
        continue
      keyDict = relData[-1][1][ 'features' ][-1][1]
      for key in ( 'BUGFIX', 'BUG', 'FIX', "CHANGE", "NEW", "FEATURE" ):
        if line.find( "%s:" % key ) == 0:
          line = line[ len( key ) + 2: ].strip()
        elif line.find( "%s " % key ) == 0:
           line = line[ len( key ) + 1: ].strip()
        else:
          continue

        if key in ( 'BUGFIX', 'BUG', 'FIX' ):
          if 'BUGFIX' not in keyDict:
            keyDict[ 'BUGFIX' ] = []
          keyDict[ 'BUGFIX' ].append( line )
        elif key == 'CHANGE':
          if 'CHANGE' not in keyDict:
            keyDict[ 'CHANGE' ] = []
          keyDict[ 'CHANGE' ].append( line )
        elif key in ( 'NEW', 'FEATURE' ):
          if 'FEATURE' not in keyDict:
            keyDict[ 'FEATURE' ] = []
          keyDict[ 'FEATURE' ].append( line )

    return S_OK( relData )

  def __generateRSTFile( self, releaseData, rstFileName, versionFilter = False ):
    rstData = []
    for version, verData in releaseData:
      if versionFilter and version != versionFilter:
        continue
      versionLine = "Version %s" % version
      rstData.append( "" )
      rstData.append( "=" * len( versionLine ) )
      rstData.append( versionLine )
      rstData.append( "=" * len( versionLine ) )
      rstData.append( "" )
      if verData[ 'comment' ]:
        rstData.append( "\n".join( verData[ 'comment' ] ) )
        rstData.append( "" )
      for feature, featureData in verData[ 'features' ]:
        if not featureData:
          continue
        rstData.append( feature )
        rstData.append( "=" * len( feature ) )
        rstData.append( "" )
        for key in sorted( featureData ):
          rstData.append( key.capitalize() )
          rstData.append( ":" * ( len( key ) + 5 ) )
          rstData.append( "" )
          for entry in featureData[ key ]:
            rstData.append( " - %s" % entry )
          rstData.append( "" )
    #Write releasenotes.rst
    try:
      rstFilePath = os.path.join( self.params.destination, self.params.name, rstFileName )
      fd = open( rstFilePath, "w" )
      fd.write( "\n".join( rstData ) )
      fd.close()
    except Exception, excp:
      return S_ERROR( "Could not write %s: %s" % ( rstFileName, excp ) )
    return S_OK()

  def __generateReleaseNotes( self ):
    result = self.__loadReleaseNotesFile()
    if not result[ 'OK' ]:
      return result
    releaseData = result[ 'Value' ]
    if not releaseData:
      gLogger.info( "release.notes not found. Trying to find releasenotes.rst" )
      for rstFileName in ( "releasenotes.rst", "releasehistory.rst" ):
        result = self.__compileReleaseNotes( rstFileName )
        if result[ 'OK' ]:
          gLogger.notice( "Compiled %s file!" % rstFileName )
        else:
          gLogger.warn( result[ 'Message' ] )
      return S_OK()
    gLogger.info( "Loaded release.notes" )
    for rstFileName, versionFilter in ( ( "releasenotes.rst", self.params.version ),
                                        ( "releasehistory.rst", False ) ):
      result = self.__generateRSTFile( releaseData, rstFileName,
                                       versionFilter )
      if not result[ 'OK' ]:
        gLogger.error( "Could not generate %s: %s" % ( rstFileName, result[ 'Message' ] ) )
        continue
      result = self.__compileReleaseNotes( rstFileName )
      if not result[ 'OK' ]:
        gLogger.error( "Could not compile %s: %s" % ( rstFileName,  result[ 'Message' ] ) )
        continue
      gLogger.notice( "Compiled %s file!" % rstFileName )
    return S_OK()


  def __compileReleaseNotes( self, rstFile ):
    relNotesRST = os.path.join( self.params.destination, self.params.name, rstFile )
    if not os.path.isfile( relNotesRST ):
      if self.params.relNotes:
        return S_ERROR( "Defined release notes %s do not exist!" % self.params.relNotes )
      return S_ERROR( "No release notes found in %s. Skipping" % relNotesRST )
    try:
      import docutils.core
    except ImportError:
      return S_ERROR( "Docutils is not installed. Please install and rerun" )
    #Find basename
    baseNotesPath = relNotesRST
    for ext in ( '.rst', '.txt' ):
      if relNotesRST[ -len( ext ): ] == ext:
        baseNotesPath = relNotesRST[ :-len( ext ) ]
        break
    #To HTML
    try:
      fd = open( relNotesRST )
      rstData = fd.read()
      fd.close()
    except Exception, excp:
      return S_ERROR( "Could not read %s: %s" % ( relNotesRST, excp ) )
    try:
      parts = docutils.core.publish_parts( rstData, writer_name = 'html' )
    except Exception, excp:
      return S_ERROR( "Cannot generate the html %s: %s" % ( baseNotesPath, str( excp ) ) )
    baseList = [ baseNotesPath ]
    if self.params.outRelNotes:
      print "ASDAS"
      gLogger.notice( "Leaving a copy of the release notes outside the tarballs" )
      baseList.append( "%s/releasenotes.%s.%s" % ( self.params.destination, self.params.name, self.params.version ) )
    for baseFileName in baseList:
      htmlFileName = baseFileName + ".html"
      try:
        fd = open( htmlFileName, "w" )
        fd.write( parts[ 'whole' ] )
        fd.close()
      except Exception, excp:
        return S_ERROR( "Could not write %s: %s" % ( htmlFileName, excp ) )
      #To pdf
      pdfCmd = "rst2pdf '%s' -o '%s.pdf'" % ( relNotesRST, baseFileName )
      gLogger.verbose( "Executing %s" % pdfCmd )
      if os.system( pdfCmd ):
        gLogger.warn( "Could not generate PDF version of %s" % baseNotesPath )
    #Unlink if not necessary
    if not cliParams.relNotes:
      try:
        os.unlink( relNotesRST )
      except:
        pass
    return S_OK()

  def __generateTarball( self ):
    destDir = self.params.destination
    tarName = "%s-%s.tar.gz" % ( self.params.name, self.params.version )
    tarfilePath = os.path.join( destDir, tarName )
    dirToTar = os.path.join( self.params.destination, self.params.name )
    result = Distribution.writeVersionToInit( dirToTar, self.params.version )
    if not result[ 'OK' ]:
      return result
    result = Distribution.createTarball( tarfilePath, dirToTar )
    if not result[ 'OK' ]:
      return S_ERROR( "Could not generate tarball: %s" % result[ 'Error' ] )
    #Remove package dir
    shutil.rmtree( dirToTar )
    gLogger.info( "Tar file %s created" % tarName )
    return S_OK( tarfilePath )

  def create( self ):
    if not isinstance( self.params, TarModuleCreator.Params ):
      return S_ERROR( "Argument is not a TarModuleCreator.Params object " )
    result = self.params.isOK()
    if not result[ 'OK' ]:
      return result
    result = self.__checkDestination()
    if not result[ 'OK' ]:
      return result
    result = self.__checkoutSource()
    if not result[ 'OK' ]:
      return result
    result = self.__generateReleaseNotes()
    if not result[ 'OK' ]:
      gLogger.error( "Won't generate release notes: %s" % result[ 'Message' ] )
    return self.__generateTarball()

if __name__ == "__main__":
  cliParams = TarModuleCreator.Params()

  Script.disableCS()
  Script.addDefaultOptionValue( "/DIRAC/Setup", "Dummy" )
  Script.registerSwitch( "v:", "version=", "version to tar", cliParams.setVersion )
  Script.registerSwitch( "u:", "source=", "VCS path to retrieve sources from", cliParams.setSourceURL )
  Script.registerSwitch( "D:", "destination=", "Destination where to build the tar files", cliParams.setDestination )
  Script.registerSwitch( "n:", "name=", "Tarball name", cliParams.setName )
  Script.registerSwitch( "z:", "vcs=", "VCS to use to retrieve the sources (try to find out if not specified)", cliParams.setVCS )
  Script.registerSwitch( "b:", "branch=", "VCS branch (if needed)", cliParams.setVCSBranch )
  Script.registerSwitch( "p:", "path=", "VCS path (if needed)", cliParams.setVCSPath )
  Script.registerSwitch( "K:", "releasenotes=", "Path to the release notes", cliParams.setReleaseNotes )
  Script.registerSwitch( "A",  "notesoutside", "Leave a copy of the compiled release notes outside the tarball", cliParams.setOutReleaseNotes )


  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                      '\nUsage:',
                                      '  %s <option> ...\n' % Script.scriptName,
                                      '  A source, name and version are required to build the tarball',
                                      '  For instance:',
                                      '     %s -n DIRAC -v v1r0 -z svn -u http://svnweb.cern.ch/guest/dirac/DIRAC/tags/DIRAC/v1r0' % Script.scriptName ] ) )

  Script.parseCommandLine( ignoreErrors = False )

  result = cliParams.isOK()
  if not result[ 'OK' ]:
    gLogger.error( result[ 'Message' ] )
    Script.showHelp()
    sys.exit( 1 )

  tmc = TarModuleCreator( cliParams )
  result = tmc.create()
  if not result[ 'OK' ]:
    gLogger.error( "Could not create the tarball: %s" % result[ 'Message' ] )
    sys.exit( 1 )
  gLogger.always( "Tarball successfully created at %s" % result[ 'Value' ] )
