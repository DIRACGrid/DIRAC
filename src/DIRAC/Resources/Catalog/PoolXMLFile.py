""" The POOL XML File module provides a means to extract the GUID of a file or list
    of files by searching for an appropriate POOL XML Catalog in the specified directory.
"""
import os
import glob
import tarfile

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Resources.Catalog.PoolXMLCatalog import PoolXMLCatalog
from DIRAC.Core.Utilities.List import uniqueElements
from DIRAC.Core.Utilities.File import makeGuid

#############################################################################


def getGUID(fileNames, directory=""):
    """This function searches the directory for POOL XML catalog files and extracts the GUID.

    fileNames can be a string or a list, directory defaults to PWD.
    """

    if not directory:
        directory = os.getcwd()

    if not os.path.isdir(directory):
        return S_ERROR(f"{directory} is not a directory")

    if not isinstance(fileNames, list):
        fileNames = [fileNames]

    gLogger.verbose(f"Will look for POOL XML Catalog GUIDs in {directory} for {', '.join(fileNames)}")

    finalCatList = _getPoolCatalogs(directory)

    # Create POOL catalog with final list of catalog files and extract GUIDs
    generated = []
    pfnGUIDs = {}
    catalog = PoolXMLCatalog(finalCatList)
    for fname in fileNames:
        guid = str(catalog.getGuidByPfn(fname))
        if not guid:
            guid = makeGuid(fname)
            generated.append(fname)

        pfnGUIDs[fname] = guid

    if not generated:
        gLogger.info(f"Found GUIDs from POOL XML Catalogue for all files: {', '.join(fileNames)}")
    else:
        gLogger.info(f"GUIDs not found from POOL XML Catalogue (and were generated) for: {', '.join(generated)}")

    result = S_OK(pfnGUIDs)
    result["directory"] = directory
    result["generated"] = generated
    return result


#############################################################################


def getType(fileNames, directory=""):
    """This function searches the directory for POOL XML catalog files and extracts the type of the pfn.

    fileNames can be a string or a list, directory defaults to PWD.
    """

    if not directory:
        directory = os.getcwd()

    if not os.path.isdir(directory):
        return S_ERROR(f"{directory} is not a directory")

    if not isinstance(fileNames, list):
        fileNames = [fileNames]

    gLogger.verbose(f"Will look for POOL XML Catalog file types in {directory} for {', '.join(fileNames)}")

    finalCatList = _getPoolCatalogs(directory)

    # Create POOL catalog with final list of catalog files and extract GUIDs
    generated = []
    pfnTypes = {}
    catalog = PoolXMLCatalog(finalCatList)
    for fname in fileNames:
        typeFile = str(catalog.getTypeByPfn(fname))
        if not typeFile:
            typeFile = "ROOT"
            generated.append(fname)

        pfnTypes[fname] = typeFile

    if not generated:
        gLogger.info(f"Found Types from POOL XML Catalogue for all files: {', '.join(fileNames)}")
    else:
        gLogger.info(f"GUIDs not found from POOL XML Catalogue (and were generated) for: {', '.join(generated)}")

    result = S_OK(pfnTypes)
    result["directory"] = directory
    result["generated"] = generated
    return result


#############################################################################


def _getPoolCatalogs(directory=""):
    patterns = ["*.xml", "*.xml*gz"]
    omissions = [r"\.bak$"]  # to be ignored for production files

    # First obtain valid list of unpacked catalog files in directory
    poolCatalogList = []

    for pattern in patterns:
        fileList = glob.glob(os.path.join(directory, pattern))
        for fname in fileList:
            if fname.endswith(".bak"):
                gLogger.verbose(f"Ignoring BAK file: {fname}")
            elif tarfile.is_tarfile(fname):
                gLogger.debug(f"Unpacking catalog XML file {os.path.join(directory, fname)}")
                with tarfile.open(os.path.join(directory, fname), "r") as tf:
                    for member in tf.getmembers():
                        tf.extract(member, directory)
                        poolCatalogList.append(os.path.join(directory, member.name))
            else:
                poolCatalogList.append(fname)

    poolCatalogList = uniqueElements(poolCatalogList)

    # Now have list of all XML files but some may not be Pool XML catalogs...
    finalCatList = []
    for possibleCat in poolCatalogList:
        try:
            _cat = PoolXMLCatalog(possibleCat)
            finalCatList.append(possibleCat)
        except Exception as x:
            gLogger.debug(f"Ignoring non-POOL catalogue file {possibleCat}")

    gLogger.debug(f"Final list of catalog files are: {', '.join(finalCatList)}")

    return finalCatList


#############################################################################
