"""
Compile the externals
"""
import platform


# Command line interface
def getPlatformString():
    # Modified to return our desired platform string, R. Graciani
    platformTuple = (platform.system(), platform.machine())
    if platformTuple[0] == "Linux":
        platformTuple += ("-".join(platform.libc_ver()),)
    elif platformTuple[0] == "Darwin":
        platformTuple += (".".join(platform.mac_ver()[0].split(".")[:2]),)
    else:
        platformTuple += platform.release()

    platformString = "%s_%s_%s" % platformTuple

    return platformString


_gPlatform = None
_gPlatformTuple = None


def getPlatform():
    global _gPlatform, _gPlatformTuple

    if _gPlatform is None:
        _gPlatform = getPlatformString()
        _gPlatformTuple = tuple(_gPlatform.split("_"))

    return _gPlatform


def getPlatformTuple():
    getPlatform()
    return _gPlatformTuple
