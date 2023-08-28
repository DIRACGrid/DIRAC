#!/usr/bin/env python
########################################################################
# File :    dirac-dms-add-file
# Author :  Stuart Paterson
########################################################################
"""
Upload a file to the grid storage and register it in the File Catalog

Usage:
  dirac-dms-add-file [options] ... LFN Path SE [GUID]

Arguments:
  LFN:      Logical File Name
  Path:     Local path to the file
  SE:       DIRAC Storage Element
  GUID:     GUID to use in the registration (optional)

**OR**

Usage:
  dirac-dms-add-file [options] ... LocalFile ...

Arguments:
  LocalFile: Path to local file containing all the above, i.e.::

  lfn1 localfile1 SE [GUID1]
  lfn2 localfile2 SE [GUID2]

Example:
  $ dirac-dms-add-file LFN:/formation/user/v/vhamar/Example.txt Example.txt DIRAC-USER
  {'Failed': {},
   'Successful': {'/formationes/user/v/vhamar/Example.txt': {'put': 0.70791220664978027,
                                                             'register': 0.61061787605285645}}}
"""
import os
from DIRAC import S_OK
from DIRAC.Core.Base.Script import Script

overwrite = False


def setOverwrite(arg):
    global overwrite
    overwrite = True
    return S_OK()


def getDict(item_list):
    """
    From the input list, populate the dictionary
    """
    lfn_dict = {}
    lfn_dict["lfn"] = item_list[0].replace("LFN:", "").replace("lfn:", "")
    lfn_dict["localfile"] = item_list[1]
    lfn_dict["SE"] = item_list[2]
    guid = None
    if len(item_list) > 3:
        guid = item_list[3]
    lfn_dict["guid"] = guid
    return lfn_dict


@Script()
def main():
    global overwrite
    Script.registerSwitch("f", "force", "Force overwrite of existing file", setOverwrite)
    Script.parseCommandLine(ignoreErrors=True)
    args = Script.getPositionalArgs()
    if len(args) < 1 or len(args) > 4:
        Script.showHelp(exitCode=1)

    from DIRAC.DataManagementSystem.Client.DataManager import DataManager
    from DIRAC import gLogger
    import DIRAC

    exitCode = 0

    lfns = []
    if len(args) == 1:
        inputFileName = args[0]
        if os.path.exists(inputFileName):
            inputFile = open(inputFileName)
            for line in inputFile:
                line = line.rstrip()
                items = line.split()
                items[0] = items[0].replace("LFN:", "").replace("lfn:", "")
                lfns.append(getDict(items))
            inputFile.close()
        else:
            gLogger.error(f"Error: LFN list '{inputFileName}' missing.")
            exitCode = 4
    else:
        lfns.append(getDict(args))

    dm = DataManager()
    for lfn in lfns:
        if not os.path.exists(lfn["localfile"]):
            gLogger.error(f"File {lfn['localfile']} must exist locally")
            exitCode = 1
            continue
        if not os.path.isfile(lfn["localfile"]):
            gLogger.error(f"{lfn['localfile']} is not a file")
            exitCode = 2
            continue

        gLogger.notice(f"\nUploading {lfn['lfn']}")
        res = dm.putAndRegister(lfn["lfn"], lfn["localfile"], lfn["SE"], lfn["guid"], overwrite=overwrite)
        if not res["OK"]:
            exitCode = 3
            gLogger.error(f"Error: failed to upload {lfn['lfn']} to {lfn['SE']}: {res}")
            continue
        else:
            gLogger.notice(f"Successfully uploaded file to {lfn['SE']}")

    DIRAC.exit(exitCode)


if __name__ == "__main__":
    main()
