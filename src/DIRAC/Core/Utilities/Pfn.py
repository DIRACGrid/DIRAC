"""
:mod: Pfn

.. module: Pfn

:synopsis: pfn URI (un)parsing

.. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

# # imports
import os

# # from DIRAC
from DIRAC import S_OK, S_ERROR, gLogger
from six.moves.urllib import parse as urlparse


def pfnunparse(pfnDict, srmSpecific=True):
    """Wrapper for backward compatibility
    Redirect either to the old hand made style of unparsing
    the pfn, which works for srm, or to the standard one
    which seems to work for the rest

    :param srmSpecific: use the srm specific parser (default True)
    """
    if srmSpecific:
        return srm_pfnunparse(pfnDict)
    return default_pfnunparse(pfnDict)


def srm_pfnunparse(pfnDict):
    """
    Create PFN URI from pfnDict

    :param dict pfnDict:
    """
    # # make sure all keys are in
    allDict = dict.fromkeys(["Protocol", "Host", "Port", "WSUrl", "Path", "FileName"], "")
    if not isinstance(pfnDict, dict):
        return S_ERROR("pfnunparse: wrong type for pfnDict argument, expected a dict, got %s" % type(pfnDict))
    allDict.update(pfnDict)
    pfnDict = allDict

    # # c
    # # /a/b/c
    filePath = os.path.normpath("/" + pfnDict["Path"] + "/" + pfnDict["FileName"]).replace("//", "/")

    # # host
    uri = pfnDict["Host"]
    if pfnDict["Host"]:
        if pfnDict["Port"]:
            # host:port
            uri = "%s:%s" % (pfnDict["Host"], pfnDict["Port"])
        if pfnDict["WSUrl"]:
            if "?" in pfnDict["WSUrl"] and "=" in pfnDict["WSUrl"]:  # pylint: disable=unsupported-membership-test
                # host/wsurl
                # host:port/wsurl
                uri = "%s%s" % (uri, pfnDict["WSUrl"])
            else:
                # host/wsurl
                # host:port/wsurl
                uri = "%s%s?=" % (uri, pfnDict["WSUrl"])

    if pfnDict["Protocol"]:
        if uri:
            # proto://host
            # proto://host:port
            # proto://host:port/wsurl
            uri = "%s://%s" % (pfnDict["Protocol"], uri)
        else:
            # proto:
            uri = "%s:" % pfnDict["Protocol"]

    pfn = "%s%s" % (uri, filePath)

    # c
    # /a/b/c
    # proto:/a/b/c
    # proto://host/a/b/c
    # proto://host:port/a/b/c
    # proto://host:port/wsurl/a/b/c
    return S_OK(pfn)


def default_pfnunparse(pfnDict):
    """
    Create PFN URI from pfnDict

    :param dict pfnDict:
    """

    try:
        if not isinstance(pfnDict, dict):
            return S_ERROR("pfnunparse: wrong type for pfnDict argument, expected a dict, got %s" % type(pfnDict))
        allDict = dict.fromkeys(["Protocol", "Host", "Port", "Path", "FileName", "Options"], "")
        allDict.update(pfnDict)

        scheme = allDict["Protocol"]

        netloc = allDict["Host"]
        if allDict["Port"]:
            netloc += ":%s" % allDict["Port"]

        path = os.path.join(allDict["Path"], allDict["FileName"])
        query = allDict["Options"]

        pr = urlparse.ParseResult(scheme=scheme, netloc=netloc, path=path, params="", query=query, fragment="")

        pfn = pr.geturl()

        return S_OK(pfn)

    except Exception as e:  # pylint: disable=broad-except
        errStr = "Pfn.default_pfnunparse: Exception while unparsing pfn: %s" % pfnDict
        gLogger.exception(errStr, lException=e)
        return S_ERROR(errStr)


def pfnparse(pfn, srmSpecific=True):
    """Wrapper for backward compatibility
    Redirect either to the old hand made style of parsing
    the pfn, which works for srm, or to the standard one
    which seems to work for the rest

    :param srmSpecific: use the srm specific parser (default True)
    """
    if srmSpecific:
        return srm_pfnparse(pfn)
    return default_pfnparse(pfn)


def srm_pfnparse(pfn):
    """
    Parse pfn and save all bits of information into dictionary

    :param str pfn: pfn string
    """
    if not pfn:
        return S_ERROR("wrong 'pfn' argument value in function call, expected non-empty string, got %s" % str(pfn))
    pfnDict = dict.fromkeys(["Protocol", "Host", "Port", "WSUrl", "Path", "FileName"], "")
    try:
        if ":" not in pfn:
            # pfn = /a/b/c
            pfnDict["Path"] = os.path.dirname(pfn)
            pfnDict["FileName"] = os.path.basename(pfn)
        else:
            # pfn = protocol:/a/b/c
            # pfn = protocol://host/a/b/c
            # pfn = protocol://host:port/a/b/c
            # pfn = protocol://host:port/wsurl?=/a/b/c
            pfnDict["Protocol"] = pfn[0 : pfn.index(":")]
            # # remove protocol:
            pfn = pfn[len(pfnDict["Protocol"]) :]
            # # remove :// or :
            pfn = pfn[3:] if pfn.startswith("://") else pfn[1:]
            if pfn.startswith("/"):
                # # /a/b/c
                pfnDict["Path"] = os.path.dirname(pfn)
                pfnDict["FileName"] = os.path.basename(pfn)
            else:
                # # host/a/b/c
                # # host:port/a/b/c
                # # host:port/wsurl?=/a/b/c
                if ":" not in pfn:
                    # # host/a/b/c
                    pfnDict["Host"] = pfn[0 : pfn.index("/")]
                    pfn = pfn[len(pfnDict["Host"]) :]
                    pfnDict["Path"] = os.path.dirname(pfn)
                    pfnDict["FileName"] = os.path.basename(pfn)
                else:
                    # # host:port/a/b/c
                    # # host:port/wsurl?=/a/b/c
                    pfnDict["Host"] = pfn[0 : pfn.index(":")]
                    # # port/a/b/c
                    # # port/wsurl?=/a/b/c
                    pfn = pfn[len(pfnDict["Host"]) + 1 :]
                    pfnDict["Port"] = pfn[0 : pfn.index("/")]
                    # # /a/b/c
                    # # /wsurl?=/a/b/c
                    pfn = pfn[len(pfnDict["Port"]) :]
                    WSUrl = pfn.find("?")
                    WSUrlEnd = pfn.find("=")
                    if WSUrl == -1 and WSUrlEnd == -1:
                        # # /a/b/c
                        pfnDict["Path"] = os.path.dirname(pfn)
                        pfnDict["FileName"] = os.path.basename(pfn)
                    else:
                        # # /wsurl?blah=/a/b/c
                        pfnDict["WSUrl"] = pfn[0 : WSUrlEnd + 1]
                        # # /a/b/c
                        pfn = pfn[len(pfnDict["WSUrl"]) :]
                        pfnDict["Path"] = os.path.dirname(pfn)
                        pfnDict["FileName"] = os.path.basename(pfn)
        return S_OK(pfnDict)
    except Exception:  # pylint: disable=broad-except
        errStr = "Pfn.srm_pfnparse: Exception while parsing pfn: " + str(pfn)
        gLogger.exception(errStr)
        return S_ERROR(errStr)


def default_pfnparse(pfn):
    """
      Parse pfn and save all bits of information into dictionary

    :param str pfn: pfn string
    """
    if not pfn:
        return S_ERROR("wrong 'pfn' argument value in function call, expected non-empty string, got %s" % str(pfn))
    pfnDict = dict.fromkeys(["Protocol", "Host", "Port", "WSUrl", "Path", "FileName"], "")
    try:

        parse = urlparse.urlparse(pfn)
        pfnDict["Protocol"] = parse.scheme
        if ":" in parse.netloc:
            pfnDict["Host"], pfnDict["Port"] = parse.netloc.split(":")
        else:
            pfnDict["Host"] = parse.netloc
        pfnDict["Path"] = os.path.dirname(parse.path)
        pfnDict["FileName"] = os.path.basename(parse.path)
        if parse.query:
            pfnDict["Options"] = parse.query
        return S_OK(pfnDict)
    except Exception as e:  # pylint: disable=broad-except
        errStr = "Pfn.default_pfnparse: Exception while parsing pfn: " + str(pfn)
        gLogger.exception(errStr, lException=e)
        return S_ERROR(errStr)
