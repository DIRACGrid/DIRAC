"""
The Grid module contains several utilities for grid operations
"""
import os

from DIRAC.Core.Utilities.Os import sourceEnv
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.ConfigurationSystem.Client.Helpers import Local
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR
from DIRAC.Core.Utilities.Subprocess import systemCall, shellCall


def executeGridCommand(proxy, cmd, gridEnvScript=None):
    """
    Execute cmd tuple after sourcing GridEnv
    """
    currentEnv = dict(os.environ)

    if not gridEnvScript:
        # if not passed as argument, use default from CS Helpers
        gridEnvScript = Local.gridEnv()

    if gridEnvScript:
        command = gridEnvScript.split()
        ret = sourceEnv(10, command)
        if not ret["OK"]:
            return S_ERROR("Failed sourcing GridEnv: %s" % ret["Message"])
        gridEnv = ret["outputEnv"]
        #
        # Preserve some current settings if they are there
        #
        if "X509_VOMS_DIR" in currentEnv:
            gridEnv["X509_VOMS_DIR"] = currentEnv["X509_VOMS_DIR"]
        if "X509_CERT_DIR" in currentEnv:
            gridEnv["X509_CERT_DIR"] = currentEnv["X509_CERT_DIR"]
    else:
        gridEnv = currentEnv

    if not proxy:
        res = getProxyInfo()
        if not res["OK"]:
            return res
        gridEnv["X509_USER_PROXY"] = res["Value"]["path"]
    elif isinstance(proxy, str):
        if os.path.exists(proxy):
            gridEnv["X509_USER_PROXY"] = proxy
        else:
            return S_ERROR("Can not treat proxy passed as a string")
    else:
        ret = gProxyManager.dumpProxyToFile(proxy)
        if not ret["OK"]:
            return ret
        gridEnv["X509_USER_PROXY"] = ret["Value"]

    result = systemCall(120, cmd, env=gridEnv)
    return result


def ldapsearchBDII(filt=None, attr=None, host=None, base=None, selectionString="Glue"):
    """Python wrapper for ldapserch at bdii.

    :param  filt: Filter used to search ldap, default = '', means select all
    :param  attr: Attributes returned by ldapsearch, default = '*', means return all
    :param  host: Host used for ldapsearch, default = 'cclcgtopbdii01.in2p3.fr:2170', can be changed by $LCG_GFAL_INFOSYS

    :return: standard DIRAC answer with Value equals to list of ldapsearch responses

    Each element of list is dictionary with keys:

      'dn':                 Distinguished name of ldapsearch response
      'objectClass':        List of classes in response
      'attr':               Dictionary of attributes
    """

    if filt is None:
        filt = ""
    if attr is None:
        attr = ""
    if host is None:
        host = "cclcgtopbdii01.in2p3.fr:2170"
    if base is None:
        base = "Mds-Vo-name=local,o=grid"

    if isinstance(attr, list):
        attr = " ".join(attr)

    cmd = f'ldapsearch -x -LLL -o ldif-wrap=no -H ldap://{host} -b {base} "{filt}" {attr}'
    result = shellCall(0, cmd)

    response = []

    if not result["OK"]:
        return result

    status = result["Value"][0]
    stdout = result["Value"][1]
    stderr = result["Value"][2]

    if status != 0:
        return S_ERROR(stderr)

    lines = []
    for line in stdout.split("\n"):
        if line.find(" ") == 0:
            lines[-1] += line.strip()
        else:
            lines.append(line.strip())

    record = None
    for line in lines:
        if line.find("dn:") == 0:
            record = {
                "dn": line.replace("dn:", "").strip(),
                "objectClass": [],
                "attr": {"dn": line.replace("dn:", "").strip()},
            }
            response.append(record)
            continue
        if record:
            if line.find("objectClass:") == 0:
                record["objectClass"].append(line.replace("objectClass:", "").strip())
                continue
            if line.find(selectionString) == 0:
                index = line.find(":")
                if index > 0:
                    attr = line[:index]
                    value = line[index + 1 :].strip()
                    if attr in record["attr"]:
                        if isinstance(record["attr"][attr], list):
                            record["attr"][attr].append(value)
                        else:
                            record["attr"][attr] = [record["attr"][attr], value]
                    else:
                        record["attr"][attr] = value

    return S_OK(response)
