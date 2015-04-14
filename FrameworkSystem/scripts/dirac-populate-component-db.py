#!/usr/bin/env python

"""
Populates the database with the current installations of components
Requires the local dirac.cfg to contain the full list of host 
under Registry/Hosts
"""

__RCSID__ = "$Id$"

import sys
from datetime import datetime
from DIRAC.ConfigurationSystem.Client.CSAPI import CSAPI
from DIRAC.Core.Base import Script
from DIRAC.FrameworkSystem.Client.SystemAdministratorIntegrator \
  import SystemAdministratorIntegrator
from DIRAC.FrameworkSystem.Client.ComponentMonitoringClient \
  import ComponentMonitoringClient

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                        'Usage:',
                        '  %s [option|cfgfile] ... [debug]' % Script.scriptName,
                        'Arguments:',
                        '  debug:  Debug mode on/off'] ) )

Script.parseCommandLine()
args = Script.getPositionalArgs()

if len( args ) > 0 and args[0] == 'debug':
  debug = True
else:
  debug = False

componentType = ''

# Retrieve information from all the hosts
client = SystemAdministratorIntegrator()
resultAll = client.getOverallStatus()
resultHosts = client.getHostInfo()
resultInfo = client.getInfo()
resultAllDB = client.getDatabases()
resultAvailableDB = client.getAvailableDatabases()

if not resultAll[ 'OK' ]:
  print resultAll[ 'Message' ]
elif not resultHosts[ 'OK' ]:
  print resultHosts[ 'Message' ]
elif not resultAllDB[ 'OK' ]:
  print resultAllDB[ 'Message' ]
elif not resultAvailableDB[ 'OK' ]:
  print resultAvailableDB[ 'Message' ]
else:
  records = []
  for host in resultAll[ 'Value' ]:
    result = resultAll[ 'Value' ][ host ]
    hostResult = resultHosts[ 'Value' ][ host ]
    allDBResult = resultAllDB[ 'Value' ][ host ]
    availableDBResult = resultAvailableDB[ 'Value' ][ host ]

    if not result[ 'OK' ]:
      if debug:
        print 'Host %s: %s' % ( host, result[ 'Message' ] )
      continue
    elif not hostResult[ 'OK' ]:
      if debug:
        print 'Host %s: %s' % ( host, hostResult[ 'Message' ] )
      continue
    elif not allDBResult[ 'OK' ]:
      if debug:
        print 'Host %s: %s' % ( host, allDBResult[ 'Message' ] )
      continue
    elif not availableDBResult[ 'OK' ]:
      if debug:
        print 'Host %s: %s' % ( host, availableDBResult[ 'Message' ] )
      continue

    cpu = hostResult[ 'Value' ][ 'CPUModel' ].lstrip().rstrip()
    rDict = result[ 'Value' ]
    # Components other than databases
    for compType in rDict:
      if componentType and componentType != compType:
        continue
      for system in rDict[ compType ]:
        components = rDict[ compType ][ system ].keys()
        components.sort()
        for component in components:
          record = { 'Installation': {}, 'Component': {}, 'Host': {} }
          if rDict[ compType ][ system ][ component ][ 'Installed' ]:
            runitStatus = \
                str( rDict[ compType ][ system ][ component ][ 'RunitStatus' ] )
            if runitStatus != 'Unknown':
              module = \
                    str( rDict[ compType ][ system ][ component ][ 'Module' ] )
              record[ 'Component' ][ 'System' ] = system
              record[ 'Component' ][ 'Module' ] = module
              record[ 'Component' ][ 'Type' ] = compType.lower()[ : -1 ]
              record[ 'Host' ][ 'HostName' ] = host
              record[ 'Host' ][ 'CPU' ] = cpu
              record[ 'Installation' ][ 'Instance' ] = component
              record[ 'Installation' ][ 'InstallationTime' ] = datetime.now()
              records.append( record )

    # Databases
    csClient = CSAPI()
    cfg = csClient.getCurrentCFG()[ 'Value' ]
    setup = cfg.getOption( 'DIRAC/Setup' )

    allDB = allDBResult[ 'Value' ]
    availableDB = availableDBResult[ 'Value' ]

    for db in allDB:
      # Check for DIRAC only databases
      if db in availableDB.keys():
        # Check for 'installed' databases
        isSection = cfg.isSection \
                      ( 'Systems/' + availableDB[ db ][ 'System' ] + '/' +
                        cfg.getOption( 'DIRAC/Setups/' + setup + '/' +
                        availableDB[ db ][ 'System' ] ) + '/Databases/' + db +
                       '/' )
        if isSection:
          record = { 'Installation': {}, 'Component': {}, 'Host': {} }
          record[ 'Component' ][ 'System' ] = availableDB[ db ][ 'System' ]
          record[ 'Component' ][ 'Module' ] = db
          record[ 'Component' ][ 'Type' ] = 'DB'
          record[ 'Host' ][ 'HostName' ] = host
          record[ 'Host' ][ 'CPU' ] = cpu
          record[ 'Installation' ][ 'Instance' ] = db
          record[ 'Installation' ][ 'InstallationTime' ] = datetime.now()
          records.append( record )

monitoringClient = ComponentMonitoringClient()

# Add the installations to the database
for record in records:
  result = monitoringClient.addInstallation \
    ( record[ 'Installation' ], record[ 'Component' ], record[ 'Host' ], True )
  if not result[ 'OK' ] and debug:
    print result[ 'Message' ]
