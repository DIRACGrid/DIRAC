#!/usr/bin/env python
########################################################################
# File :    dirac-accounting-decode-fileid
# Author :  Adria Casajus
########################################################################
"""
Decode Accounting plot URLs

Usage:
  dirac-accounting-decode-fileid [options] ... URL ...

Arguments:
  URL: encoded URL of a DIRAC Accounting plot
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import sys
import pprint
from six.moves.urllib_parse import parse_qs
from six.moves.urllib import parse as urlparse

from DIRAC import gLogger
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.DIRACScript import DIRACScript


@DIRACScript()
def main():
  from DIRAC.Core.Utilities.Plotting.FileCoding import extractRequestFromFileId
  Script.parseCommandLine()

  fileIds = Script.getPositionalArgs()

  for fileId in fileIds:
    # Try to find if it's a url
    parseRes = urlparse.urlparse(fileId)
    if parseRes.query:
      queryRes = parse_qs(parseRes.query)
      if 'file' in queryRes:
        fileId = queryRes['file'][0]
    # Decode
    result = extractRequestFromFileId(fileId)
    if not result['OK']:
      gLogger.error("Could not decode fileId", "'%s', error was %s" % (fileId, result['Message']))
      sys.exit(1)
    gLogger.notice("Decode for '%s' is:\n%s" % (fileId, pprint.pformat(result['Value'])))

  sys.exit(0)


if __name__ == "__main__":
    main()
