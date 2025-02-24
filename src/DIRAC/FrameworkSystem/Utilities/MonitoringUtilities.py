"""
Utilities for ComponentMonitoring features
"""
import datetime
import socket

from DIRAC import S_OK
from DIRAC.FrameworkSystem.Client.ComponentMonitoringClient import ComponentMonitoringClient


def monitorInstallation(componentType, system, component, module=None, cpu=None, hostname=None, user=None):
    """
    Register the installation of a component in the InstalledComponentsDB
    """
    monitoringClient = ComponentMonitoringClient()

    if not module:
        module = component

    # Retrieve user installing the component
    if not user:
        user = "unknown"

    if not cpu:
        cpu = "Not available"
        for line in open("/proc/cpuinfo"):
            if line.startswith("model name"):
                cpu = line.split(":")[1][0:64]
                cpu = cpu.replace("\n", "").lstrip().rstrip()

    if not hostname:
        hostname = socket.getfqdn()
    instance = component[0:32]

    result = monitoringClient.installationExists(
        {"Instance": instance, "UnInstallationTime": None},
        {"Type": componentType, "DIRACSystem": system, "DIRACModule": module},
        {"HostName": hostname, "CPU": cpu},
    )

    if not result["OK"]:
        return result
    if result["Value"]:
        return S_OK(f"Monitoring of {component} is already enabled")

    result = monitoringClient.addInstallation(
        {"InstallationTime": datetime.datetime.utcnow(), "InstalledBy": user, "Instance": instance},
        {"Type": componentType, "DIRACSystem": system, "DIRACModule": module},
        {"HostName": hostname, "CPU": cpu},
        True,
    )
    return result


def monitorUninstallation(system, component, cpu=None, hostname=None, user=None):
    """
    Register the uninstallation of a component in the InstalledComponentsDB
    """
    monitoringClient = ComponentMonitoringClient()

    # Retrieve user uninstalling the component
    if not user:
        user = "unknown"

    if not cpu:
        cpu = "Not available"
        for line in open("/proc/cpuinfo"):
            if line.startswith("model name"):
                cpu = line.split(":")[1][0:64]
                cpu = cpu.replace("\n", "").lstrip().rstrip()

    if not hostname:
        hostname = socket.getfqdn()
    instance = component[0:32]

    result = monitoringClient.updateInstallations(
        {"Instance": instance, "UnInstallationTime": None},
        {"DIRACSystem": system},
        {"HostName": hostname, "CPU": cpu},
        {"UnInstallationTime": datetime.datetime.utcnow(), "UnInstalledBy": user},
    )
    return result
