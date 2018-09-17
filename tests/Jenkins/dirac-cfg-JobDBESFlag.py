#!/usr/bin/env python
""" update local cfg
"""

from __future__ import absolute_import
from DIRAC.Core.Base import Script

Script.setUsageMessage('\n'.join([__doc__.split('\n')[1],
                                  'Usage:',
                                  '  %s [option|cfgFile] ... DB ...' % Script.scriptName]))

Script.parseCommandLine()

args = Script.getPositionalArgs()
useESBackend = args[0]

# now updating the CS

from DIRAC.ConfigurationSystem.Client.CSAPI import CSAPI
csAPI = CSAPI()

csAPI.setOption('Systems/WorkloadManagement/dirac-JenkinsSetup/Services/JobMonitoring/useES', '%s' % useESBackend)

csAPI.commit()
