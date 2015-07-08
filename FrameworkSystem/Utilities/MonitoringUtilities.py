"""
Utilities for ComponentMonitoring features
"""

import datetime
import socket
from DIRAC import gConfig, S_OK, S_ERROR
from DIRAC.FrameworkSystem.Client.ComponentMonitoringClient import ComponentMonitoringClient

def monitorInstallation( componentType, system, component, module = None, cpu = None, hostname = None ):
  """
  Register the installation of a component in the ComponentMonitoringDB
  """
  monitoringClient = ComponentMonitoringClient()

  if not module:
    module = component

  if not cpu:
    cpu = 'Not available'
    for line in open( '/proc/cpuinfo' ):
      if line.startswith( 'model name' ):
        cpu = line.split( ':' )[1][ 0 : 64 ]
        cpu = cpu.replace( '\n', '' ).lstrip().rstrip()

  if not hostname:
    hostname = socket.getfqdn()
  instance = component[ 0 : 32 ]

  result = monitoringClient.installationExists \
                        ( { 'Instance': instance,
                            'UnInstallationTime': None },
                          { 'Type': componentType,
                            'System': system,
                            'Module': module },
                          { 'HostName': hostname,
                            'CPU': cpu } )

  if not result[ 'OK' ]:
    return result
  if result[ 'Value' ]:
    return S_OK( 'Monitoring of %s is already enabled' % component )

  result = monitoringClient.addInstallation \
                            ( { 'InstallationTime': datetime.datetime.utcnow(),
                                'Instance': instance },
                              { 'Type': componentType,
                                'System': system,
                                'Module': module },
                              { 'HostName': hostname,
                                'CPU': cpu },
                              True )
  return result

def monitorUninstallation( system, component, cpu = None, hostname = None ):
  """
  Register the uninstallation of a component in the ComponentMonitoringDB
  """
  monitoringClient = ComponentMonitoringClient()

  if not cpu:
    cpu = 'Not available'
    for line in open( '/proc/cpuinfo' ):
      if line.startswith( 'model name' ):
        cpu = line.split( ':' )[1][0:64]
        cpu = cpu.replace( '\n', '' ).lstrip().rstrip()

  if not hostname:
    hostname = socket.getfqdn()
  instance = component[ 0 : 32 ]

  result = monitoringClient.updateInstallations \
                        ( { 'Instance': instance, 'UnInstallationTime': None },
                          { 'System': system },
                          { 'HostName': hostname, 'CPU': cpu },
                          { 'UnInstallationTime': datetime.datetime.utcnow() } )
  return result
