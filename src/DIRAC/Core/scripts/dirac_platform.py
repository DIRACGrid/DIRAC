#!/usr/bin/env python
########################################################################
# File :   dirac-platform
# Author : Adria Casajus
########################################################################
"""
The *dirac-platform* script determines the "platform" of a certain node.
The platform is a string used to identify the minimal characteristics of the node,
enough to determine which version of DIRAC can be installed.

On a RHEL 6 node, for example, the determined dirac platform is "Linux_x86_64_glibc-2.5"

Usage:
  dirac-platform [options]

Example:
  $ dirac-platform
  Linux_x86_64_glibc-2.5

"""
try:
    from DIRAC.Core.Utilities.Platform import getPlatformString
except Exception:
    import argparse
    import platform
    import os
    import sys
    import re
    import subprocess

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.parse_known_args()

    # We need to patch python platform module. It does a string comparison for the libc versions.
    # it fails when going from 2.9 to 2.10,
    # the fix converts the version to a tuple and attempts a numeric comparison

    _libc_search = re.compile(r"(__libc_init)" "|" "(GLIBC_([0-9.]+))" "|" r"(libc(_\w+)?\.so(?:\.(\d[0-9.]*))?)")

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
                    lib = "libc"
                elif glibc:
                    glibcversion_parts = glibcversion.split(".")
                    for i in range(len(glibcversion_parts)):
                        try:
                            glibcversion_parts[i] = int(glibcversion_parts[i])
                        except ValueError:
                            glibcversion_parts[i] = 0
                    if libcinit and not lib:
                        lib = "libc"
                    elif glibc:
                        if lib != "glibc":
                            lib = "glibc"
                            version = glibcversion_parts
                        elif glibcversion_parts > version:
                            version = glibcversion_parts
                elif so:
                    if lib != "glibc":
                        lib = "libc"
                        version = max(version, soversion)
                        if threads and version[-len(threads) :] != threads:
                            version = version + threads
                pos = m.end()
        return lib, ".".join(map(str, version))

    # Command line interface
    def getPlatformString():
        # Modified to return our desired platform string, R. Graciani
        platformTuple = (platform.system(), platform.machine())
        if platformTuple[0] == "Linux":
            sp = subprocess.Popen(["/sbin/ldconfig", "--print-cache"], stdout=subprocess.PIPE)
            ldre = re.compile(r".*=> (.*/libc\.so\..*$)")
            libs = []
            for line in sp.stdout.readlines():
                reM = ldre.match(line)
                if reM:
                    libs.append(reM.groups()[0])
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

    def main():
        print(getPlatformString())

else:
    from DIRAC.Core.Base.Script import Script

    @Script()
    def main():
        print(getPlatformString())


if __name__ == "__main__":
    main()
