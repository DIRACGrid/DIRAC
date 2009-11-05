#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :   dirac-config-ce
# Author : Ricardo Graciani
########################################################################
__RCSID__   = "$Id$"
__VERSION__ = "$Revision: 1.1 $"
from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script
from DIRAC import S_OK, gConfig, gLogger

configFile = gConfig.diracConfigFilePath

ceName = ''
ceType = ''

def setCEName( args ):
  global ceName
  ceName = args
  return S_OK()

def setCEType( args ):
  global ceType
  ceType = args
  return S_OK()

Script.registerSwitch( "N:", "Name=", "Computing Element Name", setCEName )
Script.registerSwitch( "T:", "Type=", "Computing Element Type", setCEType )

Script.parseCommandLine( ignoreErrors = True )

if not ceName or not ceType:
  gLogger.error( 'Missing Mandatory Argument "-N CEName" or "-T CEType"' )
  Script.localCfg._LocalConfiguration__showHelp()

gLogger.info( 'Configuring CE:', ceName )

from DIRAC.Core.Utilities.CFG import CFG
cfg = CFG()
cfg.loadFromFile(configFile)

localsiteCfg = cfg['LocalSite']
if localsiteCfg.existsKey( ceName ):
  gLogger.info(' Removing existing configuration', localsiteCfg.deleteKey( ceName ) )
localsiteCfg.createNewSection( ceName )

ceCfg = localsiteCfg[ceName]

cfgSection = '/Scripts/%s' % Script.localCfg.componentName
gConfig.setOptionValue( cfgSection+'/CEType', ceType )

configDict = gConfig.getOptionsDict(cfgSection)['Value']
configOptions = configDict.keys()
configOptions.sort()

# Add script command line options to CE Configuration
for option in configOptions:
  value = configDict[option]
  gLogger.info(' Adding CE option', '%s = %s' % ( option, value ) )
  ceCfg.setOption( option, value )

# Now ResourceDict (if it exists)
resSection = cfgSection + '/ResourceDict'
resDict = gConfig.getOptionsDict( resSection )
if resDict['OK']:
  ceCfg.createNewSection('ResourceDict')
  resCfg = ceCfg['ResourceDict']
  resDict = resDict['Value']
  resOptions = resDict.keys()
  resOptions.sort()
  for option in resOptions:
    value = resDict[option]
    gLogger.info(' Adding ResourceDict option ', '%s = %s' % ( option, value ) )
    resCfg.setOption( option, value )

# Now load this cfg into gConfig and try to instantiate the CE
gConfig.loadCFG( cfg )
from DIRAC.Resources.Computing.ComputingElementFactory    import ComputingElementFactory
ceFactory = ComputingElementFactory( ceName )
try:
  ceInstance = ceFactory.getCE()
except Exception, x:
  DIRAC.abort( -1, 'Fail to instantiate CE', x )
if not ceInstance['OK']:
  DIRAC.abort( -1, 'Fail to instantiate CE', ceInstance['Message'] )

# Everything is OK, we can save the new cfg (for the moment let's copy by hand
newConfigFile = configFile+'.new'
cfg.writeToFile( newConfigFile )
gLogger.always( 'Copy %s to %s for final installation' % ( newConfigFile, configFile ) )


