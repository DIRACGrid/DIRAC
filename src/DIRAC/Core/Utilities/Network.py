"""
   Collection of DIRAC useful network related modules
   by default on Error they return None
"""
import socket
import os
from urllib import parse

import psutil

from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR


def discoverInterfaces():
    interfaces = {k: {a.family: a.address for a in v} for k, v in psutil.net_if_addrs().items()}
    return {
        k: {"ip": v.get(socket.AF_INET, "0.0.0.0"), "mac": v.get(psutil.AF_LINK, "00:00:00:00:00:00")}
        for k, v in interfaces.items()
    }


def getFQDN():
    sFQDN = socket.getfqdn()
    if sFQDN.find("localhost") > -1:
        sFQDN = os.uname()[1]
        socket.getfqdn(sFQDN)
    return sFQDN


def splitURL(url):
    o = parse.urlparse(url)
    if o.scheme == "":
        return S_ERROR("'%s' URL is missing protocol" % url)
    path = o.path
    path = path.lstrip("/")
    return S_OK((o.scheme, o.hostname or "", o.port or 0, path))


def getIPsForHostName(hostName):
    try:
        ips = [t[4][0] for t in socket.getaddrinfo(hostName, 0)]
    except Exception as e:
        return S_ERROR(f"Can't get info for host {hostName}: {str(e)}")
    uniqueIPs = []
    for ip in ips:
        if ip not in uniqueIPs:
            uniqueIPs.append(ip)
    return S_OK(uniqueIPs)


def checkHostsMatch(host1, host2):
    ipLists = []
    for host in (host1, host2):
        result = getIPsForHostName(host)
        if not result["OK"]:
            return result
        ipLists.append(result["Value"])
    # Check
    for ip1 in ipLists[0]:
        if ip1 in ipLists[1]:
            return S_OK(True)
    return S_OK(False)
