#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-admin-sort-cs-sites
# Author :  Matvey Sapunov
########################################################################
"""
  Sort sites by their name in CS. Sort can be done alphabetically or by country.
  By default it's alphabetical sort (i.e. LCG.IHEP.cn, LCG.IHEP.su, LCG.IN2P3.fr)
  
  Options:
    -C --Country              Sort sites alphabetically with respect to country (i.e. LCG.IHEP.cn, LCG.IN2P3.fr, LCG.IHEP.su)
    
  Argument:
    -S --Section              Name of the subsection in the CS '/Resources/Sites/' section to be sorted (ie: LCG, DIRAC)
    
  Example: dirac-admin-sort-cs-sites -C --Section DIRAC
  Sort sites in subsection /Resources/Sites/DIRAC with respect to countries
"""
__RCSID__ = "$Id$"

import DIRAC
from DIRAC                                                   import gLogger
from DIRAC.Core.Base                                         import Script
from DIRAC.Core.Security.ProxyInfo                           import getProxyInfo
from DIRAC.ConfigurationSystem.Client.Helpers.Registry       import getPropertiesForGroup
from DIRAC.ConfigurationSystem.Client.CSAPI                  import CSAPI
from DIRAC.Core.Utilities.Time                               import dateTime , toString

global SORTBYNAME
SORTBYNAME = True

def sortBy( arg ):
  global SORTBYNAME
  SORTBYNAME = False

def byCountry( arg ):
  cb = arg.split( "." )
  if not len( cb ) == 3:
    gLogger.error( "%s is not in GRID.NAME.COUNTRY format " )
    return False
  return cb[ 2 ]

Script.registerSwitch( "C", "country", "Alphabetical sort by countries first (i.e. LCG.IHEP.cn, LCG.IN2P3.fr, LCG.IHEP.su)" , sortBy )

Script.setUsageMessage( "\n".join( [ __doc__.split( "\n" )[ 1 ]
                                      ,"Usage:"
                                      ,"  %s [option|cfgfile] <Section>" % Script.scriptName
                                      ,"Optional arguments:"
                                      ,"  Section:       Name of the subsection in '/Resources/Sites/' CS section to be sorted (i.e. LCG DIRAC)"
                                      ,"Example:"
                                      ,"  dirac-admin-sort-cs-sites -C CLOUDS DIRAC"
                                      ,"  sort sites by country in '/Resources/Sites/CLOUDS' and '/Resources/Sites/DIRAC' subsection"
                                      ,"" ] ) )
                                     
Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

result = getProxyInfo()
if not result[ "OK" ]:
  gLogger.error( "Failed to get proxy information", result[ "Message" ] )
  DIRAC.exit( 2 )
proxy = result[ "Value" ]
if proxy[ "secondsLeft" ] < 1:
  gLogger.error( "Your proxy has expired, please create new one" )
  DIRAC.exit( 2 )
group = proxy[ "group" ]
if not "CSAdministrator" in getPropertiesForGroup( group ):
   gLogger.error( "You must be CSAdministrator user to execute this script" )
   gLogger.notice( "Please issue 'dirac-proxy-init -g [group with CSAdministrator Property]'" )
   DIRAC.exit( 2 )

cs = CSAPI()
result = cs.getCurrentCFG()
if not result[ "OK" ]:
  gLogger.error( "Failed to get copy of CS", result[ "Message" ] )
  DIRAC.exit( 2 )
cfg = result[ "Value" ]

if not cfg.isSection( "Resources" ):
  gLogger.error( "Section '/Resources' is absent in CS" )
  DIRAC.exit( 2 )

if not cfg.isSection( "Resources/Sites" ):
  gLogger.error( "Subsection '/Resources/Sites' is absent in CS" )
  DIRAC.exit( 2 )


if args and len( args ) > 0:
  resultList = args[ : ]
else:
  resultList = cfg[ "Resources" ][ "Sites" ].listSections()

hasRun = False
dirty = False
for i in resultList:
  if not cfg.isSection( "Resources/Sites/%s" % i ):
    gLogger.error( "Subsection /Resources/Sites/%s does not exists" % i )
    continue
  hasRun = True
  if SORTBYNAME:
    dirty = cfg[ "Resources" ][ "Sites" ][ i ].sortAlphabetically()
  else:
    dirty = cfg[ "Resources" ][ "Sites" ][ i ].sortBy( key = byCountry )

if not hasRun:
  gLogger.notice( "Failed to find suitable subsections with sitenames to sort" )
  DIRAC.exit( 0 )

if not dirty:
  gLogger.notice( "Nothing to do, sitenames are already sorted" )
  DIRAC.exit( 0 )

timestamp = toString( dateTime() )
stamp = "Sitenames are sorted by %s script at %s" % ( Script.scriptName , timestamp )
cs.setOptionComment( "/Resources/Sites" , stamp )

result = cs.commit()
if not result[ "OK" ]:
  gLogger.error( "Failed to commit changes to CS", result[ "Message" ] )
  DIRAC.exit( 2 )
gLogger.notice( "Sitenames are sorted and commited to CS" )
DIRAC.exit( 0 )
