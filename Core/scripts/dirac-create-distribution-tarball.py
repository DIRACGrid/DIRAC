#!/usr/bin/env python
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

  def __checkDestination( self, params ):
    if not params.destination:
      params.destination = tempfile.mkdtemp( 'DIRACTarball' )

    gLogger.notice( "Will generate tarball in %s" % params.destination )

    if not os.path.isdir( params.destination ):
      try:
        os.makedirs( params.destination )
      except Exception, e:
        return S_ERROR( "Cannot write to destination: %s" % str( e ) )

    return S_OK()

  def __discoverVCS( self, params ):
    sourceURL = params.sourceURL
    if os.path.expanduser( sourceURL ).find( "/" ) == 0:
      sourceURL = os.path.expanduser( sourceURL )
      params.vcs = "file"
      return True
    if sourceURL.find( ":" ) == 0:
      params.vcs = "cvs"
      return True
    if sourceURL.find( ".git" ) == len( sourceURL ) - 4:
      params.vcs = "git"
      return True
    for vcs in TarModuleCreator.VALID_VCS:
      if sourceURL.find( vcs ) == 0:
        params.vcs = vcs
        return True
    return False

  def __checkoutSource( self, params ):
    if not params.vcs:
      if not self.__discoverVCS( params ):
        return S_ERROR( "Could not autodiscover VCS" )
    gLogger.info( "Checking out using %s method" % params.vcs )

    if params.vcs == "file":
      return self.__checkoutFromFile( params )
    elif params.vcs == "cvs":
      return self.__checkoutFromCVS( params )
    elif params.vcs == "svn":
      return self.__checkoutFromSVN( params )
    elif params.vcs == "hg":
      return self.__checkoutFromHg( params )
    elif params.vcs == "git":
      return self.__checkoutFromGit( params )

    return S_ERROR( "OOPS. Unknown VCS %s!" % params.vcs )

  def __checkoutFromFile( self, params ):
    sourceURL = params.sourceURL
    if sourceURL.find( "file://" ) == 0:
      sourceURL = sourceURL[ 7: ]
    sourceURL = os.path.realpath( sourceURL )
    try:
      pyVer = sys.version_info
      if pyVer[0] == 2 and pyVer[1] < 6:
        shutil.copytree( sourceURL,
                         os.path.join( params.destination, params.name ),
                         symlinks = True )
      else:
        shutil.copytree( sourceURL,
                         os.path.join( params.destination, params.name ),
                         symlinks = True,
                         ignore = shutil.ignore_patterns( '.svn', '.git', '.hg', '*.pyc', '*.pyo', 'CVS' ) )
    except Exception, e:
      return S_ERROR( "Could not copy data from source URL: %s" % str( e ) )
    return S_OK()

  def __checkoutFromCVS( self, params ):
    cmd = "cvs export -d '%s' '%s'" % ( params.sourceURL, os.path.join( params.destination, params.name ) )
    gLogger.verbose( "Executing: %s" % cmd )
    result = Subprocess.shellCall( 900, cmd )
    if not result[ 'OK' ]:
      return S_ERROR( "Error while retrieving sources from CVS: %s" % result[ 'Message' ] )
    exitStatus, stdData, errData = result[ 'Value' ]
    if exitStatus:
      return S_ERROR( "Error while retrieving sources from CVS: %s" % "\n".join( [ stdData, errData ] ) )
    return S_OK()

  def __checkoutFromSVN( self, params ):
    cmd = "svn export --trust-server-cert --non-interactive '%s' '%s'" % ( params.sourceURL, os.path.join( params.destination, params.name ) )
    gLogger.verbose( "Executing: %s" % cmd )
    result = Subprocess.shellCall( 900, cmd )
    if not result[ 'OK' ]:
      return S_ERROR( "Error while retrieving sources from SVN: %s" % result[ 'Message' ] )
    exitStatus, stdData, errData = result[ 'Value' ]
    if exitStatus:
      return S_ERROR( "Error while retrieving sources from SVN: %s" % "\n".join( [ stdData, errData ] ) )
    return S_OK()

  def __checkoutFromHg( self, params ):
    if params.vcsBranch:
      brCmr = "-b %s" % params.vcsBranch
    else:
      brCmr = ""
    fDirName = os.path.join( params.destination, params.name )
    cmd = "hg clone %s '%s' '%s.tmp1'" % ( brCmr,
                                      params.sourceURL,
                                      fDirName )
    gLogger.verbose( "Executing: %s" % cmd )
    if os.system( cmd ):
      return S_ERROR( "Error while retrieving sources from hg" )

    hgArgs = [ "--cwd '%s.tmp1'" % fDirName ]
    if params.vcsPath:
      hgArgs.append( "--include '%s/*'" % params.vcsPath )
    hgArgs.append( "'%s.tmp2'" % fDirName )

    cmd = "hg archive %s" % " ".join( hgArgs )
    gLogger.verbose( "Executing: %s" % cmd )
    exportRes = os.system( cmd )
    shutil.rmtree( "%s.tmp1" % fDirName )

    if exportRes:
      return S_ERROR( "Error while exporting from hg" )

    #TODO: tmp2/path to dest
    source = "%s.tmp2" % fDirName
    if params.vcsPath:
      source = os.path.join( source, params.vcsPath )

    if not os.path.isdir( source ):
      shutil.rmtree( "%s.tmp2" % fDirName )
      return S_ERROR( "Path %s does not exist in repo" )

    os.rename( source, fDirName )
    shutil.rmtree( "%s.tmp2" % fDirName )

    return S_OK()

  def __checkoutFromGit( self, params ):
    if params.vcsBranch:
      brCmr = "-b %s" % params.vcsBranch
    else:
      brCmr = ""
    fDirName = os.path.join( params.destination, params.name )
    cmd = "git clone %s '%s' '%s'" % ( brCmr,
                                           params.sourceURL,
                                           fDirName )
    gLogger.verbose( "Executing: %s" % cmd )
    if os.system( cmd ):
      return S_ERROR( "Error while retrieving sources from git" )

    branchName = "DIRACDistribution-%s" % os.getpid()

    cmd = "( cd '%s'; git checkout -b '%s' '%s' )" % ( fDirName, branchName, params.version )

    gLogger.verbose( "Executing: %s" % cmd )
    exportRes = os.system( cmd )
    shutil.rmtree( "%s/.git" % fDirName )

    if exportRes:
      return S_ERROR( "Error while exporting from git" )

    return S_OK()

  def __generateReleaseNotes( self, params ):
    if not params.relNotes:
      relNotesPath = os.path.join( params.destination, params.name, "releasenotes.rst" )
    else:
      relNotesPath = params.relNotes
    if not os.path.isfile( relNotesPath ):
      if params.relNotes:
        return S_ERROR( "Defined release notes %s do not exist!" % params.relNotes )
      gLogger.notice( "No release notes found in %s. Skipping" % relNotesPath )
      return S_OK()
    try:
      import docutils.core
    except ImportError:
      return S_ERROR( "Docutils is not installed. Please install and rerun" )
    #Find basename
    baseNotesPath = relNotesPath
    for ext in ( '.rst', '.txt' ):
      if relNotesPath[ -len( ext ): ] == ext:
        baseNotesPath = relNotesPath[ :-len( ext ) ]
        break
    #To HTML
    try:
      fd = open( relNotesPath )
      rstData = fd.read()
      fd.close()
    except Exception, excp:
      return S_ERROR( "Could not read %s: %s" % ( relNotesPath, excp ) )
    try:
      parts = docutils.core.publish_parts( rstData, writer_name = 'html' )
    except Exception, excp:
      return S_ERROR( "Cannot generate the html release notes: %s" % str( excp ) )
    htmlFileName = baseNotesPath + ".html"
    try:
      fd = open( htmlFileName, "w" )
      fd.write( parts[ 'whole' ] )
      fd.close()
    except Exception, excp:
      return S_ERROR( "Could not write %s: %s" % ( htmlFileName, excp ) )
    #To pdf
    if os.system( "rst2pdf '%s' -o '%s.pdf'" % ( relNotesPath, baseNotesPath ) ):
      gLogger.warn( "Could not generate PDF version of release notes" )
    if not cliParams.relNotes:
      try:
        os.unlink( relNotesPath )
      except:
        pass
    return S_OK()

  def __generateTarball( self, params ):
    destDir = params.destination
    tarName = "%s-%s.tar.gz" % ( params.name, params.version )
    tarfilePath = os.path.join( destDir, tarName )
    dirToTar = os.path.join( params.destination, params.name )
    result = Distribution.writeVersionToInit( dirToTar, params.version )
    if not result[ 'OK' ]:
      return result
    result = Distribution.createTarball( tarfilePath, dirToTar )
    if not result[ 'OK' ]:
      return S_ERROR( "Could not generate tarball: %s" % result[ 'Error' ] )
    #Remove package dir
    shutil.rmtree( dirToTar )
    gLogger.info( "Tar file %s created" % tarName )
    return S_OK( tarfilePath )

  def create( self, params ):
    if not isinstance( params, TarModuleCreator.Params ):
      return S_ERROR( "Argument is not a TarModuleCreator.Params object " )
    result = params.isOK()
    if not result[ 'OK' ]:
      return result
    result = self.__checkDestination( params )
    if not result[ 'OK' ]:
      return result
    result = self.__checkoutSource( params )
    if not result[ 'OK' ]:
      return result
    result = self.__generateReleaseNotes( params )
    if not result[ 'OK' ]:
      return result
    return self.__generateTarball( params )

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

  tmc = TarModuleCreator()
  result = tmc.create( cliParams )
  if not result[ 'OK' ]:
    gLogger.error( "Could not create the tarball: %s" % result[ 'Message' ] )
    sys.exit( 1 )
  gLogger.always( "Tarball successfully created at %s" % result[ 'Value' ] )
