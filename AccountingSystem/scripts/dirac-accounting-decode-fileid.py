#!/usr/bin/env python
########################################################################
# File :    dirac-accounting-decode-fileid
# Author :  Adria Casajus
########################################################################
"""
  Decode Accounting plot URLs
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
__RCSID__ = "$Id$"

import pprint
import sys
import urlparse
import cgi
from DIRAC import gLogger
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.Plotting.FileCoding import extractRequestFromFileId

Script.setUsageMessage('\n'.join([__doc__.split('\n')[1],
                                  'Usage:',
                                  '  %s [option|cfgfile] ... URL ...' % Script.scriptName,
                                  'Arguments:',
                                  '  URL: encoded URL of a DIRAC Accounting plot']))
Script.parseCommandLine()

fileIds = Script.getPositionalArgs()

for fileId in fileIds:
  # Try to find if it's a url
  parseRes = urlparse.urlparse(fileId)
  if parseRes.query:
    queryRes = cgi.parse_qs(parseRes.query)
    if 'file' in queryRes:
      fileId = queryRes['file'][0]
  # Decode
  result = extractRequestFromFileId(fileId)
  if not result['OK']:
    gLogger.error("Could not decode fileId", "'%s', error was %s" % (fileId, result['Message']))
    sys.exit(1)
  gLogger.notice("Decode for '%s' is:\n%s" % (fileId, pprint.pformat(result['Value'])))

sys.exit(0)
