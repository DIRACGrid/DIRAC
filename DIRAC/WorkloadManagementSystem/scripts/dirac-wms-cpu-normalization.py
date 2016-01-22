#!/usr/bin/env python
########################################################################
# File :    dirac-wms-cpu-normalization
# Author :  Ricardo Graciani
########################################################################
"""
  Determine Normalization for current CPU
"""
__RCSID__ = "$Id$"
import DIRAC
from DIRAC.Core.Base import Script

Script.registerSwitch( "U", "Update", "Update dirac.cfg with the resulting value" )
Script.registerSwitch( "R:", "Reconfig=", "Update given configuration file with the resulting value" )

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ' % Script.scriptName ] ) )

Script.parseCommandLine( ignoreErrors = True )

update = False
configFile = None

for unprocSw in Script.getUnprocessedSwitches():
  if unprocSw[0] in ( "U", "Update" ):
    update = True
  elif unprocSw[0] in ( "R", "Reconfig" ):
    configFile = unprocSw[1]


if __name__ == "__main__":

  from DIRAC.WorkloadManagementSystem.Client.CPUNormalization import getCPUNormalization, getPowerFromMJF
  from DIRAC import gLogger, gConfig

  result = getCPUNormalization()

  if not result['OK']:
    gLogger.error( result['Message'] )

  norm = round( result['Value']['NORM'], 1 )

  gLogger.notice( 'Estimated CPU power is %.1f %s' % ( norm, result['Value']['UNIT'] ) )

  mjfPower = getPowerFromMJF()
  if mjfPower:
    gLogger.notice( 'CPU power from MJF is %.1f HS06' % mjfPower )
  else:
    gLogger.notice( 'MJF not available on this node' )

  if update and not configFile:
    gConfig.setOptionValue( '/LocalSite/CPUScalingFactor', mjfPower if mjfPower else norm )
    gConfig.setOptionValue( '/LocalSite/CPUNormalizationFactor', norm )

    gConfig.dumpLocalCFGToFile( gConfig.diracConfigFilePath )
  if configFile:
    from DIRAC.Core.Utilities.CFG import CFG
    cfg = CFG()
    try:
      # Attempt to open the given file
      cfg.loadFromFile( configFile )
    except:
      pass
    # Create the section if it does not exist
    if not cfg.existsKey( 'LocalSite' ):
      cfg.createNewSection( 'LocalSite' )
    cfg.setOption( '/LocalSite/CPUScalingFactor', mjfPower if mjfPower else norm )
    cfg.setOption( '/LocalSite/CPUNormalizationFactor', norm )

    cfg.writeToFile( configFile )


  DIRAC.exit()
