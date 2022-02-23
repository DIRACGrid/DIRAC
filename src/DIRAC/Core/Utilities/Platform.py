"""
Compile the externals
"""
import platform
import sys
import os
import re

# We need to patch python platform module. It does a string comparison for the libc versions.
# it fails when going from 2.9 to 2.10,
# the fix converts the version to a tuple and attempts a numeric comparison

_libc_search = re.compile(b"(__libc_init)" b"|" b"(GLIBC_([0-9.]+))" b"|" b"(libc(_\\w+)?\\.so(?:\\.(\\d[0-9.]*))?)")


def libc_ver(executable=sys.executable, lib="", version="", chunksize=2048):
    """Tries to determine the libc version that the file executable
    (which defaults to the Python interpreter) is linked against.

    Returns a tuple of strings (lib,version) which default to the
    given parameters in case the lookup fails.

    Note that the function has intimate knowledge of how different
    libc versions add symbols to the executable and thus is probably
    only useable for executables compiled using gcc.

    The file is read and scanned in chunks of chunksize bytes.

    """
    with open(executable, "rb") as f:
        binary = f.read(chunksize)
        pos = 0
        version = [0, 0, 0]
        while True:
            m = _libc_search.search(binary, pos)
            if not m:
                binary = f.read(chunksize)
                if not binary:
                    break
                pos = 0
                continue
            libcinit, glibc, glibcversion, so, threads, soversion = m.groups()
            if libcinit and not lib:
                lib = b"libc"
            elif glibc:
                glibcversion_parts = glibcversion.split(b".")
                for i in range(len(glibcversion_parts)):
                    try:
                        glibcversion_parts[i] = int(glibcversion_parts[i])
                    except ValueError:
                        glibcversion_parts[i] = 0
                if libcinit and not lib:
                    lib = b"libc"
                elif glibc:
                    if lib != b"glibc":
                        lib = b"glibc"
                        version = glibcversion_parts
                    elif glibcversion_parts > version:
                        version = glibcversion_parts
            elif so:
                if lib != b"glibc":
                    lib = b"libc"
                    version = max(version, [int(soversion)]).pop()
                    if threads and version[-len(threads) :] != threads:
                        version = version + threads
            pos = m.end()
    return lib.decode(), ".".join(map(str, version))


# ## Command line interface


def getPlatformString():
    # Modified to return our desired platform string, R. Graciani
    platformTuple = (platform.system(), platform.machine())
    if platformTuple[0] == "Linux":
        try:
            import subprocess

            sp = subprocess.Popen(["/sbin/ldconfig", "--print-cache"], stdout=subprocess.PIPE, universal_newlines=True)
            spStdout = sp.stdout
        except Exception:
            sp = None
            spStdout = os.popen("/sbin/ldconfig --print-cache", "r")
        ldre = re.compile(r".*=> (.*/libc\.so\..*$)")
        libs = []
        for line in spStdout.readlines():
            reM = ldre.match(line)
            if reM:
                libs.append(reM.groups()[0])
        if sp:
            if "terminate" in dir(sp):
                sp.terminate()
                sp.wait()

        if not libs:
            # get version of higher libc installed
            if platform.machine().find("64") != -1:
                lib = "/lib64"
            else:
                lib = "/lib"
            for libFile in os.listdir(lib):
                if libFile.find("libc-") == 0 or libFile.find("libc.so") == 0:
                    libs.append(os.path.join(lib, libFile))
        newest_lib = [0, 0, 0]
        for lib in libs:
            lib_parts = libc_ver(lib)[1].split(".")
            for i in range(len(lib_parts)):
                try:
                    lib_parts[i] = int(lib_parts[i])
                except ValueError:
                    lib_parts[i] = 0
                    # print "non integer version numbers"
            if lib_parts > newest_lib:
                newest_lib = lib_parts

        platformTuple += ("glibc-" + ".".join(map(str, newest_lib)),)
    elif platformTuple[0] == "Darwin":
        platformTuple += (".".join(platform.mac_ver()[0].split(".")[:2]),)
    elif platformTuple[0] == "Windows":
        platformTuple += (platform.win32_ver()[0],)
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
