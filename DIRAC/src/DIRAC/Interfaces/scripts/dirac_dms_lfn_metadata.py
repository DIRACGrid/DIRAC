#!/usr/bin/env python
########################################################################
# File :    dirac-admin-lfn-metadata
# Author :  Stuart Paterson
########################################################################
"""
Obtain replica metadata from file catalogue client.

Example:
  $ dirac-dms-lfn-metadata /formation/user/v/vhamar/test.txt
  {'Failed': {},
   'Successful': {'/formation/user/v/vhamar/test.txt': {'Checksum': 'eed20d47',
                                                        'ChecksumType': 'Adler32',
                                                        'CreationDate': datetime.datetime(2011, 2, 11, 14, 52, 47),
                                                        'FileID': 250L,
                                                        'GID': 2,
                                                        'GUID': 'EDE6DDA4-3344-3F39-A993-8349BA41EB23',
                                                        'Mode': 509,
                                                        'ModificationDate': datetime.datetime(2011, 2, 11, 14, 52, 47),
                                                        'Owner': 'vhamar',
                                                        'OwnerGroup': 'dirac_user',
                                                        'Size': 34L,
                                                        'Status': 1,
                                                        'UID': 2}}}
"""
import DIRAC
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument(["LFN:      Logical File Name or file containing LFNs"])
    _, lfns = Script.parseCommandLine(ignoreErrors=True)

    from DIRAC.Interfaces.API.Dirac import Dirac

    dirac = Dirac()
    exitCode = 0
    errorList = []

    if len(lfns) == 1:
        try:
            with open(lfns[0]) as f:
                lfns = f.read().splitlines()
        except Exception:
            pass

    result = dirac.getLfnMetadata(lfns, printOutput=True)
    if not result["OK"]:
        print("ERROR: ", result["Message"])
        exitCode = 2

    DIRAC.exit(exitCode)


if __name__ == "__main__":
    main()
