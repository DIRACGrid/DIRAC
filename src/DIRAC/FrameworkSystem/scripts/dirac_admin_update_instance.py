#!/usr/bin/env python
"""
Script to apply update to all or some dirac servers and restart them
"""

import DIRAC
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    Script.registerSwitch(
        "",
        "hosts=",
        "Comma separated list of hosts or file containing row wise list of hosts"
        " targeted for update (leave empty for all)",
    )
    Script.registerSwitch(
        "",
        "excludeHosts=",
        "Comma separated list of hosts or file containing row wise list of hosts" " excluded from update",
    )
    Script.registerSwitch("", "retry=", "Number of retry attempts on hosts that have failed to update")
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument("version: version of DIRAC you want to update to")

    Script.parseCommandLine(ignoreErrors=False)

    # parseCommandLine show help when mandatory arguments are not specified or incorrect argument
    version = Script.getPositionalArgs(group=True)
    retry = 0
    hosts = []
    excludeHosts = []

    for switch in Script.getUnprocessedSwitches():
        if switch[0] == "hosts":
            hosts = switch[1]
        if switch[0] == "excludeHosts":
            excludeHosts = switch[1]
        if switch[0] == "retry":
            retry = int(switch[1])

    # Parse targeted hosts
    try:
        with open(hosts) as f:
            hosts = f.read().splitlines()
            hosts = [str(host) for host in hosts]
    except Exception:
        pass

    if not isinstance(hosts, list):
        hosts = hosts.split(",")

    # Parse excluded hosts
    try:
        with open(excludeHosts) as f:
            excludeHosts = f.read().splitlines()
            excludeHosts = [str(host) for host in excludeHosts]
    except Exception:
        pass

    if not isinstance(excludeHosts, list):
        excludeHosts = excludeHosts.split(",")

    from concurrent.futures import ThreadPoolExecutor, as_completed

    from DIRAC import S_OK, S_ERROR
    from DIRAC import gLogger
    from DIRAC.FrameworkSystem.Client.SystemAdministratorClient import SystemAdministratorClient
    from DIRAC.FrameworkSystem.Client.ComponentMonitoringClient import ComponentMonitoringClient

    def getListOfHosts():
        """
        Return the list of all hosts that constitute a DIRAC instance
        """
        client = ComponentMonitoringClient()
        result = client.getHosts({}, False, False)
        if result["OK"]:
            hosts = [host["HostName"] for host in result["Value"]]
            return S_OK(hosts)
        return S_ERROR(f"Cannot get list of hosts: {result['Message']}")

    def parseHostname(hostName):
        """
        Separate the hostname from the port

        :param str hostName: hostname you want to parse
        """
        hostList = hostName.split(":")
        host = hostList[0]
        if len(hostList) == 2:
            port = hostList[1]
        else:
            port = None
        return host, port

    def updateHost(hostName, version):
        """
        Apply update to specific host

        :param str hostName: name of the host you want to update
        :param str version: version vArBpC you want to update to
        """
        host, port = parseHostname(hostName)

        client = SystemAdministratorClient(host, port)
        result = client.ping()
        if not result["OK"]:
            gLogger.error(f"Cannot connect to {host}")
            return result

        gLogger.notice(
            f"Initiating software update of {host} to {version}, this can take a while, please be patient ..."
        )

        result = client.updateSoftware(version, timeout=600)
        if not result["OK"]:
            return result
        return S_OK()

    def updateHosts(version, hosts=[], excludeHosts=[]):
        """
        Apply update to all hosts

        :param str version: version vArBpC you want to update to
        :param list[str] hosts: list of hosts to be updated, defaults to []
        :param list[str] excludeHosts: list of hosts to be excluded from updated, defaults to []
        """
        if not hosts:
            result = getListOfHosts()
            if not result["OK"]:
                return result
            hosts = result["Value"]

        # Subtract the excluded hosts
        hosts = [host for host in hosts if host not in excludeHosts]

        updateSuccess = []
        updateFail = []

        executor = ThreadPoolExecutor(max_workers=len(hosts))
        futureUpdate = {executor.submit(updateHost, host, version): host for host in hosts}
        for future in as_completed(futureUpdate):
            host = futureUpdate[future]
            result = future.result()
            if result["OK"]:
                updateSuccess.append(host)
            else:
                updateFail.append(host)

        if not updateFail:
            gLogger.notice("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
            gLogger.notice("!!! Successfully updated all hosts !!!")
            gLogger.notice("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
            return S_OK([updateSuccess, updateFail])
        if not updateSuccess:
            gLogger.notice("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
            gLogger.notice("XXXXX Failed to update all hosts XXXXX")
            gLogger.notice("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
            return S_OK([updateSuccess, updateFail])
        gLogger.notice("X!X!X!X!X!X!X!X!X!X!X!X!X!X!X!X!X!X!")
        gLogger.notice("X!X!X Partially updated hosts X!X!X!")
        gLogger.notice("Succeeded to update:")
        for host in updateSuccess:
            gLogger.notice(f" + {host}")
        gLogger.notice("Failed to update:")
        for host in updateFail:
            gLogger.notice(f" - {host}")
        gLogger.notice("X!X!X!X!X!X!X!X!X!X!X!X!X!X!X!X!X!X!")
        return S_OK([updateSuccess, updateFail])

    def restartHost(hostName):
        """
        Restart all systems and components of a host

        :param str hostName: name of the host you want to restart
        """
        host, port = parseHostname(hostName)

        gLogger.notice(f"Pinging {host} ...")

        client = SystemAdministratorClient(host, port)
        result = client.ping()
        if not result["OK"]:
            gLogger.error(f"Could not connect to {host}: {result['Message']}")
            return result
        gLogger.notice(f"Host {host} is active")

        gLogger.notice("Initiating restart of all systems and components")
        # This restart call will always return S_ERROR because of SystemAdministrator restart
        # Connection will be lost to the host
        result = client.restartComponent("*", "*")
        if result["Message"] == "Peer closed connection":
            gLogger.notice(f"Restarted all systems on {host} : connection to SystemAdministrator lost")
            return S_OK(result["Message"])
        gLogger.error(f"Received unxpected message: {result['Message']}")
        return result

    def updateInstance(version, hosts, excludeHosts, retry):
        """
        Update each server of an instance and restart them

        :param str version: version vArBpC you want to update to
        :param list[str] hosts: list of hosts to be updated
        :param list[str] excludeHosts: list of hosts to be excluded from update
        :param int retry: number of retry attempts on hosts that have failed to update
        """
        result = updateHosts(version, hosts, excludeHosts)
        if not result["OK"]:
            return result

        updateSuccess = result["Value"][0]
        updateFail = result["Value"][1]
        restartSuccess = []
        restartFail = []
        for host in updateSuccess:
            result = restartHost(host)
            if result["OK"]:
                restartSuccess.append(host)
            else:
                restartFail.append(host)

        if not restartFail and not updateFail:
            return S_OK("Successfully updated and restarted all hosts")

        gLogger.notice("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
        gLogger.notice("XXXXX There were problems in the update process XXXXX")
        gLogger.notice("Succeeded to update:")
        for host in updateSuccess:
            gLogger.notice(f" + {host}")
        gLogger.notice("Succeeded to restart:")
        for host in restartSuccess:
            gLogger.notice(f" + {host}")
        gLogger.notice("Failed to update:")
        for host in updateFail:
            gLogger.notice(f" - {host}")
        gLogger.notice("Failed to restart:")
        for host in restartFail:
            gLogger.notice(f" - {host}")
        gLogger.notice("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")

        if retry > 0:
            retryHosts = list(set(updateFail + restartFail))
            gLogger.notice(f"Retrying update on ({retry} attempts remaining):")
            for host in retryHosts:
                gLogger.notice(f" - {host}")
            gLogger.notice("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
            return updateInstance(version, retryHosts, excludeHosts, retry - 1)

        return S_ERROR("Update failed!")

    result = updateInstance(version, hosts, excludeHosts, retry)
    if not result["OK"]:
        gLogger.fatal(result["Message"])
        DIRAC.exit(1)
    gLogger.notice(result["Value"])
    DIRAC.exit(0)


if __name__ == "__main__":
    main()
