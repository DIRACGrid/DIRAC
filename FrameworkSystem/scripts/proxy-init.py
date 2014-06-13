#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    proxy-init.py
# Author :  Adrian Casajus
########################################################################
""" 
  This is a command to do all the proxy generation related operations:
    - DIRAC proxy generation
    - proxy upload to the DIRAC ProxyManager
    - proxy upload to the MyProxy server if requested
    - voms proxy extensions generation
"""

__RCSID__ = "$Id$"

import sys
import os
import getpass
import imp
import DIRAC
from DIRAC.Core.Base import Script
from DIRAC import gConfig, gLogger
Script.disableCS()

from DIRAC.FrameworkSystem.Client.ProxyGeneration import CLIParams, generateProxy
from DIRAC.FrameworkSystem.Client.ProxyUpload import uploadProxy
from DIRAC.Core.Security import CS, Properties
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.Core.Security.MyProxy import MyProxy
from DIRAC.Core.Security.VOMS import VOMS

def uploadProxyToMyProxy( params, DNAsUsername ):
  """ Upload proxy to the MyProxy server
  """

  myProxy = MyProxy()
  if DNAsUsername:
    gLogger.verbose( "Uploading pilot proxy with group %s to %s..." % ( params.getDIRACGroup(), myProxy.getMyProxyServer() ) )
  else:
    gLogger.verbose( "Uploading user proxy with group %s to %s..." % ( params.getDIRACGroup(), myProxy.getMyProxyServer() ) )
  retVal = myProxy.getInfo( proxyInfo[ 'path' ], useDNAsUserName = DNAsUsername )
  if retVal[ 'OK' ]:
    remainingSecs = ( int( params.getProxyRemainingSecs() / 3600 ) * 3600 ) - 7200
    myProxyInfo = retVal[ 'Value' ]
    if 'timeLeft' in myProxyInfo and remainingSecs < myProxyInfo[ 'timeLeft' ]:
      gLogger.verbose( " Already uploaded" )
      return True
  retVal = generateProxy( params )
  if not retVal[ 'OK' ]:
    gLogger.error( " There was a problem generating proxy to be uploaded to myproxy: %s" % retVal[ 'Message' ] )
    return False
  retVal = getProxyInfo( retVal[ 'Value' ] )
  if not retVal[ 'OK' ]:
    gLogger.error( " There was a problem generating proxy to be uploaded to myproxy: %s" % retVal[ 'Message' ] )
    return False
  generatedProxyInfo = retVal[ 'Value' ]
  retVal = myProxy.uploadProxy( generatedProxyInfo[ 'path' ], useDNAsUserName = DNAsUsername )
  if not retVal[ 'OK' ]:
    gLogger.error( " Can't upload to myproxy: %s" % retVal[ 'Message' ] )
    return False
  gLogger.verbose( " Uploaded" )
  return True

def uploadProxyToDIRACProxyManager( params ):
  """ Upload proxy to the DIRAC ProxyManager service
  """

  gLogger.verbose( "Uploading user pilot proxy with group %s..." % ( params.getDIRACGroup() ) )
  params.onTheFly = True
  retVal = uploadProxy( params )
  if not retVal[ 'OK' ]:
    gLogger.error( " There was a problem generating proxy to be uploaded proxy manager: %s" % retVal[ 'Message' ] )
    return False
  return True


if __name__ == "__main__":

  cliParams = CLIParams()
  cliParams.registerCLISwitches()

  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                       'Usage:',
                                       '  %s [option|cfgfile] ...' % Script.scriptName, ] ) )
  Script.disableCS()
  Script.parseCommandLine()
  gConfig.setOptionValue( "/DIRAC/Security/UseServerCertificate", "no" )

  diracGroup = cliParams.getDIRACGroup()
  time = cliParams.getProxyLifeTime()

  retVal = generateProxy( cliParams )
  if not retVal[ 'OK' ]:
    gLogger.error( "Can't create a proxy: %s" % retVal[ 'Message' ] )
    sys.exit( 1 )
  gLogger.info( "Proxy created" )

  Script.enableCS()

  retVal = getProxyInfo( retVal[ 'Value' ] )
  if not retVal[ 'OK' ]:
    gLogger.error( "Can't create a proxy: %s" % retVal[ 'Message' ] )
    sys.exit( 1 )

  proxyInfo = retVal[ 'Value' ]
  if 'username' not in proxyInfo:
    print "Not authorized in DIRAC"
    sys.exit( 1 )

  retVal = CS.getGroupsForUser( proxyInfo[ 'username' ] )
  if not retVal[ 'OK' ]:
    gLogger.error( "No groups defined for user %s" % proxyInfo[ 'username' ] )
    sys.exit( 1 )
  availableGroups = retVal[ 'Value' ]

  pilotGroup = False
  for group in availableGroups:
    groupProps = CS.getPropertiesForGroup( group )
    if Properties.PILOT in groupProps or Properties.GENERIC_PILOT in groupProps:
      pilotGroup = group
      break


  myProxyFlag = gConfig.getValue( '/DIRAC/VOPolicy/UseMyProxy', False )

  issuerCert = proxyInfo[ 'chain' ].getIssuerCert()[ 'Value' ]
  remainingSecs = issuerCert.getRemainingSecs()[ 'Value' ]
  cliParams.setProxyRemainingSecs( remainingSecs - 300 )

  if not pilotGroup:
    if cliParams.strict:
      gLogger.error( "No pilot group defined for user %s" % proxyInfo[ 'username' ] )
      sys.exit( 1 )
    else:
      gLogger.warn( "No pilot group defined for user %s" % proxyInfo[ 'username' ] )
  else:
    cliParams.setDIRACGroup( pilotGroup )
    if myProxyFlag:
      uploadProxyToMyProxy( cliParams, True )
    success = uploadProxyToDIRACProxyManager( cliParams )
    if not success and cliParams.strict:
      sys.exit( 1 )

  cliParams.setDIRACGroup( proxyInfo[ 'group' ] )
  if myProxyFlag:
    uploadProxyToMyProxy( cliParams, False )
  success = uploadProxyToDIRACProxyManager( cliParams )
  if not success and cliParams.strict:
    sys.exit( 1 )

  finalChain = proxyInfo[ 'chain' ]

  vomsMapping = CS.getVOMSAttributeForGroup( proxyInfo[ 'group' ] )
  vo = CS.getVOMSVOForGroup( proxyInfo[ 'group' ] )
  if vomsMapping:
    voms = VOMS()
    retVal = voms.setVOMSAttributes( finalChain, vomsMapping, vo )
    if not retVal[ 'OK' ]:
      #print "Cannot add voms attribute %s to proxy %s: %s" % ( attr, proxyInfo[ 'path' ], retVal[ 'Message' ] )
      msg = "Warning : Cannot add voms attribute %s to proxy\n" % ( vomsMapping )
      msg += "          Accessing data in the grid storage from the user interface will not be possible.\n"
      msg += "          The grid jobs will not be affected."
      if cliParams.strict:
        gLogger.error( msg )
        sys.exit( 1 )
      gLogger.warn( msg )
    else:
      finalChain = retVal[ 'Value' ]

  retVal = finalChain.dumpAllToFile( proxyInfo[ 'path' ] )
  if not retVal[ 'OK' ]:
    gLogger.error( "Cannot write proxy to file %s" % proxyInfo[ 'path' ] )
    sys.exit( 1 )
  gLogger.notice( "done" )
  sys.exit( 0 )








