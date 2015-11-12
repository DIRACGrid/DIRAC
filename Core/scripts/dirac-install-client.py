#!/bin/env python

import urllib2
import os
import stat
import commands
import sys
import signal

class CFG:
  """ DIRAC independent version of the CFG class to interpret cfg files
  """

  def __init__( self, cfgData = "" ):
    self.__data = {}
    self.__children = {}
    if cfgData:
      self.parse( cfgData )

  def parse( self, cfgData ):
    try:
      self.__parse( cfgData )
    except:
      import traceback
      traceback.print_exc()
      raise
    return self

  def getChild( self, path ):
    child = self
    if isinstance( path, ( list, tuple ) ):
      pathList = path
    else:
      pathList = [ sec.strip() for sec in path.split( "/" ) if sec.strip() ]
    for childName in pathList:
      if childName not in child.__children:
        return False
      child = child.__children[ childName ]
    return child

  def __parse( self, cfgData, cIndex = 0 ):
    childName = ""
    numLine = 0
    while cIndex < len( cfgData ):
      eol = cfgData.find( "\n", cIndex )
      if eol < cIndex:
        #End?
        return cIndex
      numLine += 1
      if eol == cIndex:
        cIndex += 1
        continue
      line = cfgData[ cIndex : eol ].strip()
      #Jump EOL
      cIndex = eol + 1
      if not line or line[0] == "#":
        continue
      if line.find( "+=" ) > -1:
        fields = line.split( "+=" )
        opName = fields[0].strip()
        if opName in self.__data:
          self.__data[ opName ] += ', %s' % '+='.join( fields[1:] ).strip()
        else:
          self.__data[ opName ] = '+='.join( fields[1:] ).strip()
        continue

      if line.find( "=" ) > -1:
        fields = line.split( "=" )
        self.__data[ fields[0].strip() ] = "=".join( fields[1:] ).strip()
        continue

      opFound = line.find( "{" )
      if opFound > -1:
        childName += line[ :opFound ].strip()
        if not childName:
          raise Exception( "No section name defined for opening in line %s" % numLine )
        childName = childName.strip()
        self.__children[ childName ] = CFG()
        eoc = self.__children[ childName ].__parse( cfgData, cIndex )
        cIndex = eoc
        childName = ""
        continue

      if line == "}":
        return cIndex
      #Must be name for section
      childName += line.strip()
    return cIndex

  def createSection( self, name, cfg = False ):
    if isinstance( name, ( list, tuple ) ):
      pathList = name
    else:
      pathList = [ sec.strip() for sec in name.split( "/" ) if sec.strip() ]
    parent = self
    for lev in pathList[:-1]:
      if lev not in parent.__children:
        parent.__children[ lev ] = CFG()
      parent = parent.__children[ lev ]
    secName = pathList[-1]
    if secName not in parent.__children:
      if not cfg:
        cfg = CFG()
      parent.__children[ secName ] = cfg
    return parent.__children[ secName ]

  def isSection( self, obList ):
    return self.__exists( [ ob.strip() for ob in obList.split( "/" ) if ob.strip() ] ) == 2

  def sections( self ):
    return [ k for k in self.__children ]

  def isOption( self, obList ):
    return self.__exists( [ ob.strip() for ob in obList.split( "/" ) if ob.strip() ] ) == 1

  def options( self ):
    return [ k for k in self.__data ]

  def __exists( self, obList ):
    if len( obList ) == 1:
      if obList[0] in self.__children:
        return  2
      elif obList[0] in self.__data:
        return 1
      else:
        return 0
    if obList[0] in self.__children:
      return self.__children[ obList[0] ].__exists( obList[1:] )
    return 0

  def get( self, opName, defaultValue = None ):
    try:
      value = self.__get( [ op.strip() for op in opName.split( "/" ) if op.strip() ] )
    except KeyError:
      if defaultValue != None:
        return defaultValue
      raise
    if defaultValue == None:
      return value
    defType = type( defaultValue )
    if defType == bool:
      return value.lower() in ( "1", "true", "yes" )
    try:
      return defType( value )
    except ValueError:
      return defaultValue


  def __get( self, obList ):
    if len( obList ) == 1:
      if obList[0] in self.__data:
        return self.__data[ obList[0] ]
      raise KeyError( "Missing option %s" % obList[0] )
    if obList[0] in self.__children:
      return self.__children[ obList[0] ].__get( obList[1:] )
    raise KeyError( "Missing section %s" % obList[0] )

  def getSections( self, path ):
    child = self.getChild( path )
    if child:
      return child.sections()
    else:
      return []

  def toString( self, tabs = 0 ):
    lines = [ "%s%s = %s" % ( "  " * tabs, opName, self.__data[ opName ] ) for opName in self.__data ]
    for secName in self.__children:
      lines.append( "%s%s" % ( "  " * tabs, secName ) )
      lines.append( "%s{" % ( "  " * tabs ) )
      lines.append( self.__children[ secName ].toString( tabs + 1 ) )
      lines.append( "%s}" % ( "  " * tabs ) )
    return "\n".join( lines )

  def getOptions( self, path = "" ):
    if path:
      parent = self.getChild( path )
    else:
      parent = self
    if not parent:
      return []
    return tuple( parent.__data )

  def delPath( self, path ):
    path = [ sec.strip() for sec in path.split( "/" ) if sec.strip() ]
    if not path:
      return
    keyName = path[ -1 ]
    parentPath = path[:-1]
    if parentPath:
      parent = self.getChild( parentPath )
    else:
      parent = self
    if parent:
      parent.__data.pop( keyName )

  def update( self, path, cfg ):
    parent = self.getChild( path )
    if not parent:
      self.createSection( path, cfg )
      return
    parent.__apply( cfg )

  def __apply( self, cfg ):
    for k in cfg.sections():
      if k in self.__children:
        self.__children[ k ].__apply( cfg.getChild( k ) )
      else:
        self.__children[ k ] = cfg.getChild( k )
    for k in cfg.options():
      self.__data[ k ] = cfg.get( k )

##############################################
# End of the CFG class definition
##############################################

DIRAC_INSTALL_URL = "https://raw.github.com/DIRACGrid/DIRAC/master/Core/scripts/dirac-install.py"
DIRAC_GLOBAL_DEFAULTS = "http://lhcbproject.web.cern.ch/lhcbproject/dist/DIRAC3/globalDefaults.cfg"

##############################################
# Helper functions
##############################################

def getInstallationHelp():
  globalDefaults = urllib2.urlopen( DIRAC_GLOBAL_DEFAULTS )
  defaultsData = globalDefaults.read()
  globalDefaults.close()
  defaultsCFG = CFG( defaultsData )

  sections = defaultsCFG.getSections( "/Installations" )
  options = defaultsCFG.getOptions( "/Installations" )
  installations = {}
  for section in sections:
    installations.setdefault( section, [] )
  for option in options:
    value = defaultsCFG.get( '/Installations/%s' % option )
    if value in installations:
      installations[value].append( option )

  print "\nAvailable pre-configured installations. Values in brackets are aliases of the main value\n"
  for installation in installations:
    print installation.ljust( 20 ),
    if installations[installation]:
      print installations[installation]
    else:
      print

  print "\nChoose one of the above installations"

def handler( signum, frame ):
  print '\nDIRAC client installation termination is forced, bye...'
  sys.exit( -1 )

##############################################
# Start the client installation
##############################################

if __name__ == "__main__":

  signal.signal( signal.SIGTERM, handler )
  signal.signal( signal.SIGINT, handler )

  if len( sys.argv ) == 2 and sys.argv[1] == "-h":
    print """
    DIRAC client installer. The installer will guide your through the process
    of the DIRAC client installation and configuration. Read and follow carefully
    the instructions
    """
    sys.exit()

  print """
  Welcome to the DIRAC client installer !

  This tool will guide you through the installation process
  asking for the necessary details.
  """

  inp = raw_input( "Do you want to continue ? [default=yes] [yes|no]: ")
  inp = inp.strip()
  if inp and inp.lower().startswith( 'n' ):
    sys.exit()

  ##############################################
  # 1. Determine the installation directory
  ##############################################

  cwdDir = os.getcwd()
  clientDir = cwdDir
  overWriteFlag = False
  doneFlag = False
  while not doneFlag:
    inp = raw_input( "\nChoose the DIRAC client installation directory\n" \
                     "[default=%s]: " % clientDir )

    inp = inp.strip()
    if inp and inp != clientDir:
      clientDir = os.path.abspath( inp )

      if not os.path.exists( clientDir ):
        os.makedirs( clientDir )

      if os.path.exists( "%s/etc/dirac.cfg" % clientDir ):
        inp = raw_input( "\nThe requested directory already contains the DIRAC client installation.\n" \
                         "Do you want to overwrite it ? [default=yes] yes|no ")
        inp = inp.strip()
        if inp.lower().startswith( 'n' ):
          inp = raw_input( "\nDo you want to choose another directory ? [default=no] yes|no " )
          inp = inp.strip()
          if inp.lower().startswith( 'y' ):
            continue
        else:
          overWriteFlag = True

      os.chdir( clientDir )
    doneFlag = True

  ##############################################
  # 2. Install the DIRAC software
  ##############################################

  doneFlag = False
  while not doneFlag:
    inp = raw_input( "\nChoose which pre-configured service your client will be connected to\n" \
                     "or choose to make a custom installation [default=custom, help=?] " )

    installConfig = "custom"
    installDefaults = '-l DIRAC -r v6r13p8 -e COMDIRAC'
    inp = inp.strip()
    if inp == "?":
      getInstallationHelp()
      continue
    if inp and inp != 'custom':
      installConfig = inp
      installDefaults = "-V %s" % installConfig
    else:
      inp = raw_input( '\nEnter the project name [default=DIRAC]: ' )
      inp = inp.strip()
      if inp:
        project = inp.strip()
      else:
        project = 'DIRAC'

      inp = raw_input( '\nEnter the project version (e.g. v6r13p8): ' )
      inp = inp.strip()
      version = inp
      if not version:
        print "please, provide a valid version"
        continue

      inp = raw_input( '\nEnter a comma separated list of extensions [default=no extensions] : ' )
      inp = inp.strip()
      extensions = [ e.strip() for e in inp.split( ',' ) ]
      extensions = ','.join( extensions )

      inp = raw_input( '\nEnter the lcg bindings version (e.g. 2015-06-26) [default=no bindings]: ' )
      inp = inp.strip()
      lcgVersion = inp

      installDefaults = "-l %s -r %s " % ( project, version )
      if extensions:
        installDefaults += "-e %s " % extensions
      if lcgVersion:
        installDefaults += "-g %s " % lcgVersion

    doneFlag = True

  print "\nInstalling DIRAC client software in %s ..." % clientDir

  installer = urllib2.urlopen( "https://raw.github.com/DIRACGrid/DIRAC/master/Core/scripts/dirac-install.py" )
  diracInstall = installer.read()
  installer.close()
  diracInstallScript = "%s/dirac-install" % clientDir
  with open( diracInstallScript, "w" ) as diracInstallFile:
    diracInstallFile.write( diracInstall )

  os.chmod( diracInstallScript , stat.S_IWUSR | stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH )
  status, output = commands.getstatusoutput( "%s/dirac-install %s" % ( clientDir, installDefaults ) )
  if status != 0:
    print "ERROR:", output
    sys.exit( 1 )

  print "Done"

  bashrcScript = "%s/bashrc" % clientDir
  sys.path.append( clientDir )

  #############################################
  # 3. Checking/installing the user certificate
  #############################################

  globusDir = os.path.expandvars( "$HOME/.globus" )
  globusFiles = os.listdir( globusDir )

  if "usercert.pem" not in globusFiles or "userkey.pem" not in globusFiles:

    input_message = """
  The user certificate is not installed.

  To install the certificate you will need a certificate file in .p12 format.
  You can obtain this file by exporting the certificate from your web browser.
  Note that you will not be able to complete the installation of the DIRAC
  client software without a properly installed certificate.

  Do you want to install the certificate now ? [default=yes] [yes|no]:
  """

    inp = raw_input( input_message )
    if not inp or inp.lower().startswith( "y" ):
      doneFlag = False
      while not doneFlag:
        inp = raw_input( "\nType the name of the certificate file in the .p12 format: " )
        p12File = inp.strip()
        p12File = os.path.expanduser( p12File )
        p12File = os.path.expandvars( p12File )
        if os.path.exists( p12File ):
          print "\nInstalling user certificate from file:", p12File
          status, output = commands.getstatusoutput( "source %s; dirac-cert-convert.sh %s" % ( bashrcScript, p12File) )
          if status != 0:
            print "ERROR:", output
            sys.exit( 1 )
        else:
          print "The certificate file %s is not found. Please retry" % p12File
          continue
        doneFlag = True

  #############################################
  # 4. Create first CS-less proxy
  #############################################

  print "\nIn order to proceed with the installation you will have to create a temporary\n" \
        "certificate proxy. You will be prompted to give the certificate pass phrase.\n"

  status, output = commands.getstatusoutput( "source %s; dirac-proxy-init -x" % bashrcScript )
  if status != 0:
    print "ERROR:", output
    sys.exit( 1 )

  #############################################
  # 5. Configuring the client
  #############################################

  configOptions = ''
  if overWriteFlag:
    configOptions = '-F '
    os.rename( "%s/etc/dirac.cfg" % clientDir, "%s/etc/dirac.cfg.bak" % clientDir )

  if installConfig == "custom":

    print "\nYou have chosen to make a custom installation. You will be prompted to provide\n" \
          "several configuration details"

    csURL = ''
    setup = ''
    extensions = []

    inp = raw_input( '\nEnter the Configuration Server: ' )
    csURL = inp.strip()
    if not csURL.endswith( "Configuration/Server" ):
      csURL = "dips://%s:9135/Configuration/Server" % csURL

    inp = raw_input( '\nEnter the DIRAC Setup: ' )
    setup = inp.strip()

    inp = raw_input( "\nSkip CA checks [default=yes] yes|no : " )
    inp = inp.strip()
    if inp and inp.lower().startswith( 'n' ):
      configOptions += "-H "

    inp = raw_input( "\nSkip CA download [default=yes] yes|no : " )
    inp = inp.strip()
    if inp and inp.lower().startswith( 'n' ):
      configOptions += "-D "

    inp = raw_input( "\nSkip VOMS download [default=yes] yes|no : " )
    inp = inp.strip()
    if inp and inp.lower().startswith( 'n' ):
      configOptions += "-M "

    print "\nConfiguring the DIRAC client as a custom installation ..."
    status, output = commands.getstatusoutput( "source %s; dirac-configure %s -C %s -S %s" % ( bashrcScript, configOptions, csURL, setup) )
    if status != 0:
      print "ERROR:", output
      sys.exit( 1 )

  else:
    print "\nConfiguring the DIRAC client as %s installation ..." % installConfig
    status, output = commands.getstatusoutput( "source %s; dirac-configure %s defaults-%s.cfg" % ( bashrcScript, configOptions, installConfig) )
    if status != 0:
      print "ERROR:", output
      sys.exit( 1 )

  #############################################
  # 6. Working proxy initialization
  #############################################

  print "\nThe DIRAC client is installed and configured. In order to start working with it\n" \
        "you have to create a working proxy."
  inp = raw_input( "Choose your DIRAC group [default=default DIRAC group] : " )
  inp = inp.strip()
  groupInput = ''
  if inp:
    groupInput = "-g %s" % inp

  status, output = commands.getstatusoutput( "source %s; dirac-proxy-init %s" % ( bashrcScript, groupInput) )
  if status != 0:
    print "ERROR:", output
    sys.exit( 1 )
  else:
    print output

  #############################################
  # 7. Finalization
  #############################################

  print "\nThe DIRAC client is installed with the following parameters:\n"

  status, output = commands.getstatusoutput( "source %s; dirac-info" % bashrcScript )
  if status != 0:
    print "ERROR:", output
  else:
    print output

  print """
  The DIRAC client installation is now completed. To start using it you will have to
  setup the client environment with the following command:

  > source %s/bashrc

  You can add this command to your .bash_profile script to execute automatically
  each time you log in.
  """ % clientDir
