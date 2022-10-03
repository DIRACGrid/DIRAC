"""Collection of DIRAC useful file related modules.

.. warning::
   By default on Error they return None.
"""

import os
import hashlib
import random
import glob
import sys
import re
import errno

# Translation table of a given unit to Bytes
# I know, it should be kB...
SIZE_UNIT_CONVERSION = {
    "B": 1,
    "KB": 1024,
    "MB": 1024 * 1024,
    "GB": 1024 * 1024 * 1024,
    "TB": 1024 * 1024 * 1024 * 1024,
    "PB": 1024 * 1024 * 1024 * 1024 * 1024,
}


def mkDir(path, mode=None):
    """Emulate 'mkdir -p path' (if path exists already, don't raise an exception)

    :param str path: directory hierarchy to create
    :param int mode: Use this mode as the mode for new directories, use python default if None.
    """
    try:
        if os.path.isdir(path):
            return
        if mode is None:
            os.makedirs(path)
        else:
            os.makedirs(path, mode)
    except OSError as osError:
        if osError.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


def mkLink(src, dst):
    """Protected creation of symbolic link"""
    try:
        os.symlink(src, dst)
    except OSError as osError:
        if osError.errno == errno.EEXIST and os.path.islink(dst) and os.path.realpath(dst) == src:
            pass
        else:
            raise


def makeGuid(fileName=None):
    """Utility to create GUID. If a filename is provided the
    GUID will correspond to its content's hexadecimal md5 checksum.
    Otherwise a random seed is used to create the GUID.
    The format is capitalized 8-4-4-4-12.

    .. warning::
       Could return None in case of OSError or IOError.

    :param string fileName: name of file
    """
    myMd5 = hashlib.md5()
    if fileName:
        try:
            with open(fileName, "rb") as fd:
                data = fd.read(10 * 1024 * 1024)
                myMd5.update(data)
        except Exception:
            return None
    else:
        myMd5.update(str(random.getrandbits(128)).encode())

    md5HexString = myMd5.hexdigest().upper()
    return generateGuid(md5HexString, "MD5")


def generateGuid(checksum, checksumtype):
    """Generate a GUID based on the file checksum"""

    if checksum:
        if checksumtype == "MD5":
            checksumString = checksum
        elif checksumtype == "Adler32":
            checksumString = str(checksum).zfill(32)
        else:
            checksumString = ""
        if checksumString:
            guid = "{}-{}-{}-{}-{}".format(
                checksumString[0:8],
                checksumString[8:12],
                checksumString[12:16],
                checksumString[16:20],
                checksumString[20:32],
            )
            guid = guid.upper()
            return guid

    # Failed to use the check sum, generate a new guid
    myMd5 = hashlib.md5()
    myMd5.update(str(random.getrandbits(128)).encode())
    md5HexString = myMd5.hexdigest()
    guid = "{}-{}-{}-{}-{}".format(
        md5HexString[0:8],
        md5HexString[8:12],
        md5HexString[12:16],
        md5HexString[16:20],
        md5HexString[20:32],
    )
    guid = guid.upper()
    return guid


def checkGuid(guid):
    """Checks whether a supplied GUID is of the correct format.
    The guid is a string of 36 characters [0-9A-F] long split into 5 parts of length 8-4-4-4-12.

    .. warning::
       As we are using GUID produced by various services and some of them could not follow
       convention, this function is passing by a guid which can be made of lower case chars or even just
       have 5 parts of proper length with whatever chars.

    :param string guid: string to be checked
    :return: True (False) if supplied string is (not) a valid GUID.
    """
    reGUID = re.compile("^[0-9A-F]{8}(-[0-9A-F]{4}){3}-[0-9A-F]{12}$")
    if reGUID.match(guid.upper()):
        return True
    else:
        guid = [len(x) for x in guid.split("-")]
        if guid == [8, 4, 4, 4, 12]:
            return True
    return False


def getSize(fileName):
    """Get size of a file.

    :param string fileName: name of file to be checked

    The os module claims only OSError can be thrown,
    but just for curiosity it's catching all possible exceptions.

    .. warning::
       On any exception it returns -1.

    """
    try:
        return os.stat(fileName)[6]
    except OSError:
        return -1


def getGlobbedTotalSize(files):
    """Get total size of a list of files or a single file.
    Globs the parameter to allow regular expressions.

    :params list files: list or tuple of strings of files
    """
    totalSize = 0
    if isinstance(files, (list, tuple)):
        for entry in files:
            size = getGlobbedTotalSize(entry)
            if size == -1:
                size = 0
            totalSize += size
    else:
        for path in glob.glob(files):
            if os.path.isdir(path) and not os.path.islink(path):
                for content in os.listdir(path):
                    totalSize += getGlobbedTotalSize(os.path.join(path, content))
            if os.path.isfile(path):
                size = getSize(path)
                if size == -1:
                    size = 0
                totalSize += size
    return totalSize


def getGlobbedFiles(files):
    """Get list of files or a single file.
    Globs the parameter to allow regular expressions.

    :params list files: list or tuple of strings of files
    """
    globbedFiles = []
    if isinstance(files, (list, tuple)):
        for entry in files:
            globbedFiles += getGlobbedFiles(entry)
    else:
        for path in glob.glob(files):
            if os.path.isdir(path) and not os.path.islink(path):
                for content in os.listdir(path):
                    globbedFiles += getGlobbedFiles(os.path.join(path, content))
            if os.path.isfile(path):
                globbedFiles.append(path)
    return globbedFiles


def getMD5ForFiles(fileList):
    """Calculate md5 for the content of all the files.

    :param fileList: list of paths
    :type fileList: python:list
    """
    fileList.sort()
    hashMD5 = hashlib.md5()
    for filePath in fileList:
        if os.path.isdir(filePath):
            continue
        with open(filePath, "rb") as fd:
            buf = fd.read(4096)
            while buf:
                hashMD5.update(buf)
                buf = fd.read(4096)
    return hashMD5.hexdigest()


def convertSizeUnits(size, srcUnit, dstUnit):
    """Converts a number from a given source unit to a destination unit.

    Example:
      In [1]: convertSizeUnits(1024, 'B', 'kB')
      Out[1]: 1

      In [2]: convertSizeUnits(1024, 'MB', 'kB')
      Out[2]: 1048576


    :param size: number to convert
    :param srcUnit: unit of the number. Any of ( 'B', 'kB', 'MB', 'GB', 'TB', 'PB')
    :param dstUnit: unit expected for the return. Any of ( 'B', 'kB', 'MB', 'GB', 'TB', 'PB')

    :returns: the size number converted in the dstUnit. In case of problem -sys.maxint is returned (negative)
    """

    srcUnit = srcUnit.upper()
    dstUnit = dstUnit.upper()

    try:
        convertedValue = float(size) * SIZE_UNIT_CONVERSION[srcUnit] / SIZE_UNIT_CONVERSION[dstUnit]
        return convertedValue

    # TypeError, ValueError: size is not a number
    # KeyError: srcUnit or dstUnit are not in the conversion list
    except (TypeError, ValueError, KeyError):
        return -sys.maxsize


if __name__ == "__main__":
    for p in sys.argv[1:]:
        print(f"{p} : {getGlobbedTotalSize(p)} bytes")
