#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :   dirac-externals-refresh
# Author : Adri
########################################################################
"""
"""
__RCSID__ = "$Id$"
#
from DIRAC.Core.Base import Script
Script.disableCS()
from DIRAC import gLogger, rootPath, S_OK, S_ERROR
from DIRAC.Core.Utilities.CFG import CFG
import os, sys
try:
  import pip
except ImportError:
  gLogger.fatal( "pip is missing! Houston, we've got a problem..." )

instType = "server"

def setInstallType( val ):
  global instType
  instType = val
  return S_OK()

Script.registerSwitch( "t:", "type=", "Installation type. 'server' by default.", setInstallType )
Script.parseCommandLine( ignoreErrors = True )

reqDict = {}

if instType.find( "client" ) == 0:
  gLogger.error( "Client installations do not support externals requirements" )
  sys.exit( 0 )

for entry in os.listdir( rootPath ):
  if len( entry ) < 5 or entry.find( "DIRAC" ) != len( entry ) - 5 :
    continue
  reqFile = os.path.join( rootPath, entry, "releases.cfg" )
  try:
   extfd = open( reqFile, "r" )
  except:
    gLogger.warn( "%s not found" % reqFile )
    continue
  reqCFG = CFG().loadFromBuffer( extfd.read() )
  extfd.close()
  reqs = reqCFG.getOption( "/RequiredExternals/%s" % instType.capitalize(), [] )
  if not reqs:
   gLogger.warn( "%s does not have requirements for %s installation" % ( entry, instType ) )
   continue
  for req in reqs:
    reqName = False
    reqCond = ""
    for cond in ( "==", ">=" ):
      iP = cond.find( req )
      if iP > 0:
        reqName = req[ :iP ]
        reqCond = req[ iP: ]
        break
    if not reqName:
      reqName = req
    if reqName not in reqDict:
      reqDict[ reqName ] = ( reqCond, entry )
    else:
      gLogger.notice( "Skipping %s, it's already requested by %s" % ( reqName, reqDict[ reqName ][1] ) )

if not reqDict:
  gLogger.notice( "Nothing to be installed" )
  sys.exit( 0 )

gLogger.notice( "Requesting installation of %s" % ", ".join( [ "%s%s" % ( reqName, reqDict[ reqName ][0] ) for reqName in reqDict ] ) )

from pip.index import PackageFinder
from pip.req import InstallRequirement, RequirementSet
from pip.locations import build_prefix, src_prefix

requirement_set = RequirementSet( 
    build_dir = build_prefix,
    src_dir = src_prefix,
    download_dir = None
    )

for reqName in reqDict:
  requirement_set.add_requirement( InstallRequirement.from_line( "%s%s" % ( reqName, reqDict[ reqName ][0] ), None ) )

install_options = []
global_options = []
finder = PackageFinder( find_links = [], index_urls = ["http://pypi.python.org/simple/"] )

requirement_set.prepare_files( finder, force_root_egg_info = False, bundle = False )
requirement_set.locate_files()
requirement_set.install( install_options, global_options )


gLogger.notice( "Installed %s" % "".join( [ str( package.name ) for package in requirement_set.successfully_installed ] ) )

