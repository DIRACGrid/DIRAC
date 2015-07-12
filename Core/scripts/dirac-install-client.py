#!/bin/env python

import urllib2
import os
import stat
import commands
import sys

print """
Welcome to the DIRAC client installation tool !

This tool will guide you through the installation process
asking for the necessary details.
"""

inp = raw_input( "Do you want to continue ? [default=yes] [yes|no]: ")
inp = inp.strip()
if not inp and inp.lower().startswith( 'n' ):
  sys.exit()

##############################################
# 1. Determine the installation directory
#############################################

clientDir = os.getcwd()
inp = raw_input( "\nChoose the DIRAC client installation directory\n" \
                 "[default=%s]: " % clientDir )

inp = inp.strip()
if inp and inp != clientDir:
  clientDir = os.path.abspath( inp )

  if not os.path.exists( clientDir ):
    os.makedirs( clientDir )
  os.chdir( clientDir )

##############################################
# 1. Install the DIRAC software
##############################################

inp = raw_input( "\nChoose which pre-configured service your client will be connected to\n" \
                 "or choose to make a custom installation [default=custom] " )

installConfig = "custom"
installDefaults = '-l DIRAC -r v6r13p8 -e COMDIRAC'
inp = inp.strip()
if inp and inp != 'custom':
  installConfig = inp
  installDefaults = "-V %s" % installConfig

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
# 2. Checking/installing the user certificate
#############################################

globusDir = os.path.expandvars( "$HOME/.globus" )
globusFiles = os.listdir( globusDir )

if "usercert.pem" not in globusFiles or "userkey.pem" not in globusFiles:

  input_message = """
The user certificate is not installed.

To install the certificate you will need a certificate file in .p12 format.
You can obtain this file by exporting the certificate from you web browser.
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
      if os.path.exists( p12File ):
        print "\nInstalling user certificate from file:", p12File
        status, output = commands.getstatusoutput( "source %s; dirac-cert-convert.sh %s" % ( bashrcScript, p12File) )
        if status != 0:
          print "ERROR:", output
          sys.exit( 1 )
      else:
        print "The certificate file %s is not found. Please retry" % p12File
        continue

#############################################
# 3. Create first CS-less proxy
#############################################

print "\nIn order to proceed with the installation you will have to create a temporary\n" \
      "certificate proxy. You will be prompted to give the certificate pass phrase.\n"

status, output = commands.getstatusoutput( "source %s; dirac-proxy-init -x" % bashrcScript )
if status != 0:
  print "ERROR:", output
  sys.exit( 1 )

#############################################
# 4. Configuring the client
#############################################

print "\nConfiguring the DIRAC client as %s installation ..." % installConfig
status, output = commands.getstatusoutput( "source %s; dirac-configure defaults-%s.cfg -ddd" % ( bashrcScript, installConfig) )
if status != 0:
  print "ERROR:", output
  sys.exit( 1 )

#############################################
# 5. Working proxy initialization
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
# 6. Finalization
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

You can add this command to you .bash_profile script to execute automatically
each time you log in.
""" % clientDir
