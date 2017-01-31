#!/usr/bin/env python
########################################################################
# File :   dirac-externals-requirements
# Author : Adri/Federico
########################################################################
""" If /RequiredExternals/ is found in releases.cfg, then some packages to install with pip may be found. This will do it.
"""

import os
import sys
from pip.index import PackageFinder
from pip.req import InstallRequirement, RequirementSet
from pip.locations import user_dir, src_prefix
from pip.download import PipSession

from DIRAC.Core.Base import Script
Script.disableCS()

from DIRAC import gLogger, rootPath, S_OK
from DIRAC.Core.Utilities.CFG import CFG

__RCSID__ = "$Id$"

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
    with open( reqFile, "r" ) as extfd:
      reqCFG = CFG().loadFromBuffer( extfd.read() )
  except:
    gLogger.warn( "%s not found" % reqFile )
    continue
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

requirement_set = RequirementSet(
    build_dir = user_dir,
    src_dir = src_prefix,
    download_dir = None,
    session = PipSession()
    )

for reqName in reqDict:
  requirement_set.add_requirement( InstallRequirement.from_line( "%s%s" % ( reqName, reqDict[ reqName ][0] ), None ) )

install_options = []
global_options = []
finder = PackageFinder( find_links = [], index_urls = ["http://pypi.python.org/simple/"] )

requirement_set.prepare_files( finder )
requirement_set.install( install_options, global_options )


gLogger.notice( "Installed %s" % "".join( [ str( package.name ) for package in requirement_set.successfully_installed ] ) )
