#!/usr/bin/env python
########################################################################
# File :    dirac_accounting_decode_fileid
# Author :  Adria Casajus
########################################################################
"""
Decode Accounting plot URLs
"""
import sys
import pprint
from urllib import parse

from DIRAC import gLogger
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    from DIRAC.Core.Utilities.Plotting.FileCoding import extractRequestFromFileId

    Script.registerArgument(["URL: encoded URL of a DIRAC Accounting plot"])

    _, fileIds = Script.parseCommandLine()

    for fileId in fileIds:
        # Try to find if it's a url
        parseRes = parse.urlparse(fileId)
        if parseRes.query:
            queryRes = parse.parse_qs(parseRes.query)
            if "file" in queryRes:
                fileId = queryRes["file"][0]
        # Decode
        result = extractRequestFromFileId(fileId)
        if not result["OK"]:
            gLogger.error("Could not decode fileId", "'{}', error was {}".format(fileId, result["Message"]))
            sys.exit(1)
        gLogger.notice("Decode for '{}' is:\n{}".format(fileId, pprint.pformat(result["Value"])))

    sys.exit(0)


if __name__ == "__main__":
    main()
