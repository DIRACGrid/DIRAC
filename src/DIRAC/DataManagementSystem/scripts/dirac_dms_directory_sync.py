#!/usr/bin/env python
########################################################################
# :file:    dirac-dms-directory-sync
# :author:  Marko Petric
########################################################################
"""
Provides basic rsync functionality for DIRAC

Syncs the source destination folder recursivly into the target destination

If option --sync is used contend that is not in the source directory but is
only in the target directory will be deleted.

Example:

  e.g.: Download
    dirac-dms-directory-sync LFN Path
  or Upload
    dirac-dms-directory-sync Path LFN SE
"""
import os
from multiprocessing import Manager

import DIRAC
from DIRAC import S_OK, S_ERROR, gConfig, gLogger
from DIRAC.Core.Base.Script import Script
from DIRAC.Core.Utilities.List import breakListIntoChunks
from DIRAC.Core.Utilities.ReturnValues import returnSingleResult


def getSetOfLocalDirectoriesAndFiles(path):
    """Return a set of all directories and subdirectories and a set of
    files contained therein for a given local path
    """

    fullPath = os.path.abspath(path)

    if not os.path.isdir(fullPath):
        return S_ERROR("The path: " + fullPath + " does not exist!")

    directories = set()
    files = set()

    for dirname, dirnames, filenames in os.walk(path):
        # add path to all subdirectories first.
        for subdirname in dirnames:
            fullSubdirname = os.path.join(dirname, subdirname)
            fullSubdirname = os.path.abspath(fullSubdirname)
            fullSubdirname = fullSubdirname.replace(fullPath, "").lstrip("/")
            directories.add(fullSubdirname)
        # add path to all filenames.
        for filename in filenames:
            fullFilename = os.path.join(dirname, filename)
            fullFilename = os.path.abspath(fullFilename)
            fullFilename = fullFilename.replace(fullPath, "").lstrip("/")
            fileSize = os.path.getsize(fullPath + "/" + fullFilename)
            if fileSize > 0:
                files.add((fullFilename, int(fileSize)))

    tree = {}
    tree["Directories"] = directories
    tree["Files"] = files

    return S_OK(tree)


def getSetOfRemoteSubDirectoriesAndFiles(path, fc, directories, files):
    """
    Recursively traverses all the subdirectories of a directory and returns a set of directories and files
    """
    result = fc.listDirectory(path)
    if result["OK"]:
        if result["Value"]["Successful"]:
            for entry in result["Value"]["Successful"][path]["Files"]:
                size = result["Value"]["Successful"][path]["Files"][entry]["MetaData"]["Size"]
                files.add((entry, size))
            for entry in result["Value"]["Successful"][path]["SubDirs"]:
                directories.add(entry)
                res = getSetOfRemoteSubDirectoriesAndFiles(entry, fc, directories, files)
                if not res["OK"]:
                    return S_ERROR("Error: " + res["Message"])
            return S_OK()
        else:
            return S_ERROR(f"Error: {result['Value']}")
    else:
        return S_ERROR("Error:" + result["Message"])


def getSetOfRemoteDirectoriesAndFiles(fc, path):
    """
    Return a set of all directories and subdirectories and the therein contained files for a given LFN
    """
    directories = set()
    files = set()

    res = getSetOfRemoteSubDirectoriesAndFiles(path, fc, directories, files)
    if not res["OK"]:
        return S_ERROR("Could not list remote directory: " + res["Message"])

    return_directories = set()
    return_files = set()

    for myfile in files:
        return_files.add((myfile[0].replace(path, "").lstrip("/"), myfile[1]))

    for mydirectory in directories:
        return_directories.add(mydirectory.replace(path, "").lstrip("/"))

    tree = {}
    tree["Directories"] = return_directories
    tree["Files"] = return_files

    return S_OK(tree)


def getContentToSync(upload, fc, source_dir, dest_dir):
    """
    Return list of files and directories to be create and deleted
    """

    if upload:
        res = getSetOfRemoteDirectoriesAndFiles(fc, dest_dir)
        if not res["OK"]:
            return S_ERROR(res["Message"])
        to_dirs = res["Value"]["Directories"]
        to_files = res["Value"]["Files"]

        res = getSetOfLocalDirectoriesAndFiles(source_dir)
        if not res["OK"]:
            return S_ERROR(res["Message"])
        from_dirs = res["Value"]["Directories"]
        from_files = res["Value"]["Files"]

    else:
        res = getSetOfLocalDirectoriesAndFiles(dest_dir)
        if not res["OK"]:
            return S_ERROR(res["Message"])
        to_dirs = res["Value"]["Directories"]
        to_files = res["Value"]["Files"]

        res = getSetOfRemoteDirectoriesAndFiles(fc, source_dir)
        if not res["OK"]:
            return S_ERROR(res["Message"])
        from_dirs = res["Value"]["Directories"]
        from_files = res["Value"]["Files"]

    # Create list of directories to delete
    dirs_delete = list(to_dirs - from_dirs)
    # Sort the list by depth of directory tree
    dirs_delete.sort(key=lambda s: -s.count("/"))
    # Create list of directories to create
    dirs_create = list(from_dirs - to_dirs)
    # Sort the list by depth of directory tree
    dirs_create.sort(key=lambda s: s.count("/"))

    # Flatten the list of pairs (filename, size) to list of filename
    files_delete = [pair[0] for pair in list(to_files - from_files)]
    files_create = [pair[0] for pair in list(from_files - to_files)]

    create = {}
    create["Directories"] = dirs_create
    create["Files"] = files_create

    delete = {}
    delete["Directories"] = dirs_delete
    delete["Files"] = files_delete

    tree = {}
    tree["Create"] = create
    tree["Delete"] = delete

    return S_OK(tree)


def removeRemoteFiles(dm, lfns):
    """
    Remove file from the catalog
    """
    for lfnList in breakListIntoChunks(lfns, 100):
        res = dm.removeFile(lfnList)
        if not res["OK"]:
            return S_ERROR("Failed to remove files:" + lfnList + res["Message"])
        else:
            return S_OK()


def removeStorageDirectoryFromSE(directory, storageElement):
    """
    Delete directory on selected storage element
    """
    from DIRAC.Resources.Storage.StorageElement import StorageElement

    se = StorageElement(storageElement, False)
    res = returnSingleResult(se.exists(directory))

    if not res["OK"]:
        return S_ERROR("Failed to obtain existence of directory" + res["Message"])

    exists = res["Value"]
    if not exists:
        return S_OK(f"The directory {directory} does not exist at {storageElement} ")

    res = returnSingleResult(se.removeDirectory(directory, recursive=True))
    if not res["OK"]:
        return S_ERROR("Failed to remove storage directory" + res["Message"])

    return S_OK()


def removeRemoteDirectory(fc, lfn):
    """
    Remove file from the catalog
    """
    storageElements = gConfig.getValue("Resources/StorageElementGroups/SE_Cleaning_List", [])

    for storageElement in sorted(storageElements):
        res = removeStorageDirectoryFromSE(lfn, storageElement)
        if not res["OK"]:
            return S_ERROR("Failed to clean storage directory at all SE:" + res["Message"])
    res = returnSingleResult(fc.removeDirectory(lfn, recursive=True))
    if not res["OK"]:
        return S_ERROR("Failed to clean storage directory at all SE:" + res["Message"])

    return S_OK("Successfully removed directory")


def createLocalDirectory(directory):
    """
    Create local directory
    """
    from DIRAC.Core.Utilities.File import mkDir

    mkDir(directory)
    if not os.path.exists(directory):
        return S_ERROR("Directory creation failed")
    return S_OK("Created directory successfully")


def removeLocalFile(path):
    """
    Remove local file
    """
    try:
        os.remove(path)
    except OSError as e:
        return S_ERROR("File deletion failed:" + e.strerror)

    if os.path.isfile(path):
        return S_ERROR("File deleting failed")
    return S_OK("Removed file successfully")


def removeLocaDirectory(path):
    """
    Remove local directory
    """
    try:
        os.rmdir(path)
    except OSError as e:
        return S_ERROR("Deleting directory failed: " + e.strerror)

    if os.path.isdir(path):
        return S_ERROR("Directory deleting failed")
    return S_OK("Removed directory successfully")


def doUpload(fc, dm, result, source_dir, dest_dir, storage, delete, nthreads):
    """
    Wrapper for uploading files
    """
    if delete:
        lfns = [dest_dir + "/" + filename for filename in result["Value"]["Delete"]["Files"]]
        if len(lfns) > 0:
            res = removeRemoteFiles(dm, lfns)
            if not res["OK"]:
                gLogger.fatal("Deleting of files: " + str(lfns) + " -X- [FAILED]" + res["Message"])
                DIRAC.exit(1)
            else:
                gLogger.notice("Deleting " + ", ".join(lfns) + " -> [DONE]")

        for directoryname in result["Value"]["Delete"]["Directories"]:
            res = removeRemoteDirectory(fc, dest_dir + "/" + directoryname)
            if not res["OK"]:
                gLogger.fatal("Deleting of directory: " + directoryname + " -X- [FAILED] " + res["Message"])
                DIRAC.exit(1)
            else:
                gLogger.notice("Deleting " + directoryname + " -> [DONE]")

    for directoryname in result["Value"]["Create"]["Directories"]:
        res = returnSingleResult(fc.createDirectory(dest_dir + "/" + directoryname))
        if not res["OK"]:
            gLogger.fatal("Creation of directory: " + directoryname + " -X- [FAILED] " + res["Message"])
            DIRAC.exit(1)
        else:
            gLogger.notice("Creating " + directoryname + " -> [DONE]")

    listOfFiles = result["Value"]["Create"]["Files"]
    # Check that we do not have too many threads
    if nthreads > len(listOfFiles):
        nthreads = len(listOfFiles)

    if nthreads == 0:
        return S_OK("Upload finished successfully")

    listOfListOfFiles = chunkList(listOfFiles, nthreads)
    res = runInParallel(
        arguments=[dm, source_dir, dest_dir, storage], listOfLists=listOfListOfFiles, function=uploadListOfFiles
    )
    if not res["OK"]:
        return S_ERROR("Upload of files failed")

    return S_OK("Upload finished successfully")


def uploadListOfFiles(dm, source_dir, dest_dir, storage, listOfFiles, tID):
    """
    Wrapper for multithreaded uploading of a list of files
    """
    log = gLogger.getLocalSubLogger(f"[Thread {tID}] ")
    threadLine = f"[Thread {tID}]"
    for filename in listOfFiles:
        destLFN = os.path.join(dest_dir, filename)
        res = returnSingleResult(dm.putAndRegister(destLFN, source_dir + "/" + filename, storage, None))
        if not res["OK"]:
            log.fatal(threadLine + " Uploading " + filename + " -X- [FAILED] " + res["Message"])
            listOfFailedFiles.append(f"{destLFN}: {res['Message']}")
        else:
            log.notice(threadLine + " Uploading " + filename + " -> [DONE]")


def doDownload(dm, result, source_dir, dest_dir, delete, nthreads):
    """
    Wrapper for downloading files
    """
    if delete:
        for filename in result["Value"]["Delete"]["Files"]:
            res = removeLocalFile(dest_dir + "/" + filename)
            if not res["OK"]:
                gLogger.fatal("Deleting of file: " + filename + " -X- [FAILED] " + res["Message"])
                DIRAC.exit(1)
            else:
                gLogger.notice("Deleting " + filename + " -> [DONE]")

        for directoryname in result["Value"]["Delete"]["Directories"]:
            res = removeLocaDirectory(dest_dir + "/" + directoryname)
            if not res["OK"]:
                gLogger.fatal("Deleting of directory: " + directoryname + " -X- [FAILED] " + res["Message"])
                DIRAC.exit(1)
            else:
                gLogger.notice("Deleting " + directoryname + " -> [DONE]")

    for directoryname in result["Value"]["Create"]["Directories"]:
        res = createLocalDirectory(dest_dir + "/" + directoryname)
        if not res["OK"]:
            gLogger.fatal("Creation of directory: " + directoryname + " -X- [FAILED] " + res["Message"])
            DIRAC.exit(1)
        else:
            gLogger.notice("Creating " + directoryname + " -> [DONE]")

    listOfFiles = result["Value"]["Create"]["Files"]
    # Chech that we do not have to many threads
    if nthreads > len(listOfFiles):
        nthreads = len(listOfFiles)

    if nthreads == 0:
        return S_OK("Upload finished successfully")

    listOfListOfFiles = chunkList(listOfFiles, nthreads)
    res = runInParallel(
        arguments=[dm, source_dir, dest_dir],
        listOfLists=listOfListOfFiles,
        function=downloadListOfFiles,
    )

    if not res["OK"]:
        return S_ERROR("Download of files failed")

    return S_OK("Upload finished successfully")


def chunkList(alist, nchunks):
    """
    Split a list into a list of equaliy sized lists
    """
    avg = len(alist) / float(nchunks)
    out = []
    last = 0.0

    while last < len(alist):
        out.append(alist[int(last) : int(last + avg)])
        last += avg

    return out


def downloadListOfFiles(dm, source_dir, dest_dir, listOfFiles, tID):
    """
    Wrapper for multithreaded downloading of a list of files
    """
    log = gLogger.getLocalSubLogger(f"[Thread {tID}] ")
    threadLine = f"[Thread {tID}]"
    for filename in listOfFiles:
        sourceLFN = os.path.join(source_dir, filename)
        res = returnSingleResult(dm.getFile(sourceLFN, dest_dir + ("/" + filename).rsplit("/", 1)[0]))
        if not res["OK"]:
            log.fatal(threadLine + " Downloading " + filename + " -X- [FAILED] " + res["Message"])
            listOfFailedFiles.append(f"{sourceLFN}: {res['Message']}")
        else:
            log.notice(threadLine + " Downloading " + filename + " -> [DONE]")


def runInParallel(arguments, listOfLists, function):
    """
    Helper for execution of uploads and downloads in parallel
    """
    from multiprocessing import Process

    processes = []
    for tID, alist in enumerate(listOfLists):
        argums = arguments + [alist] + [tID]
        pro = Process(target=function, args=argums)
        pro.start()
        processes.append(pro)
    for process in processes:
        process.join()

    if any(process.exitcode == 1 for process in processes):
        return S_ERROR()
    return S_OK()


def syncDestinations(upload, source_dir, dest_dir, storage, delete, nthreads):
    """
    Top level wrapper to execute functions
    """
    from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
    from DIRAC.DataManagementSystem.Client.DataManager import DataManager

    fc = FileCatalog()
    dm = DataManager()

    result = getContentToSync(upload, fc, source_dir, dest_dir)
    if not result["OK"]:
        return S_ERROR(result["Message"])

    if upload:
        res = doUpload(fc, dm, result, source_dir, dest_dir, storage, delete, nthreads)
        if not res["OK"]:
            return S_ERROR("Upload failed: " + res["Message"])
    else:
        res = doDownload(dm, result, source_dir, dest_dir, delete, nthreads)
        if not res["OK"]:
            return S_ERROR("Download failed: " + res["Message"])

    return S_OK("Mirroring successfully finished")


def run(parameters, delete, nthreads):
    """
    The main user interface
    """

    source_dir = parameters[0]
    dest_dir = parameters[1]
    upload = False
    storage = None

    if len(parameters) == 3:
        storage = parameters[2]
        source_dir = os.path.abspath(source_dir)
        dest_dir = dest_dir.rstrip("/")
        upload = True
        if not os.path.isdir(source_dir):
            gLogger.fatal("Source directory does not exist")
            DIRAC.exit(1)

    if len(parameters) == 2:
        dest_dir = os.path.abspath(dest_dir)
        source_dir = source_dir.rstrip("/")
        if not os.path.isdir(dest_dir):
            gLogger.fatal("Destination directory does not exist")
            DIRAC.exit(1)

    res = syncDestinations(upload, source_dir, dest_dir, storage, delete, nthreads)
    if not res["OK"]:
        return S_ERROR(res["Message"])

    return S_OK("Successfully mirrored " + source_dir + " into " + dest_dir)


@Script()
def main():
    global listOfFailedFiles

    Script.registerSwitch("D", "sync", "Make target directory identical to source")
    Script.registerSwitch("j:", "parallel=", "Multithreaded download and upload")
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument(
        (
            "LFN:           Logical File Name (Path to directory)",
            "Path:          Local path to the file (Path to directory)",
        )
    )
    Script.registerArgument(
        (
            "Path:          Local path to the file (Path to directory)",
            "LFN:           Logical File Name (Path to directory)",
        )
    )
    Script.registerArgument(" SE:            DIRAC Storage Element", mandatory=False)
    Script.parseCommandLine(ignoreErrors=False)

    args = Script.getPositionalArgs()
    if len(args) > 3:
        Script.showHelp()

    sync = False
    parallel = 1
    for switch in Script.getUnprocessedSwitches():
        if switch[0].lower() == "s" or switch[0].lower() == "sync":
            sync = True
        if switch[0].lower() == "j" or switch[0].lower() == "parallel":
            parallel = int(switch[1])

    listOfFailedFiles = Manager().list()

    # This is the execution
    returnValue = run(args, sync, parallel)
    if listOfFailedFiles:
        gLogger.error("Some file operations failed:\n\t", "\n\t".join(listOfFailedFiles))
        DIRAC.exit(1)
    if not returnValue["OK"]:
        gLogger.fatal(returnValue["Message"])
        DIRAC.exit(1)
    else:
        gLogger.notice(returnValue["Value"])
        DIRAC.exit(0)


if __name__ == "__main__":
    main()
