""" Utilities for WMS
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import io
import os
import sys
import json
import six

from DIRAC import gConfig, gLogger, S_OK
from DIRAC.Core.Utilities.File import mkDir


def createJobWrapper(jobID, jobParams, resourceParams, optimizerParams,
                     extraOptions='',
                     defaultWrapperLocation='DIRAC/WorkloadManagementSystem/JobWrapper/JobWrapperTemplate.py',
                     log=gLogger, logLevel='INFO'):
  """ This method creates a job wrapper filled with the CE and Job parameters to execute the job.
      Main user is the JobAgent
  """
  if isinstance(extraOptions, six.string_types) and extraOptions.endswith('.cfg'):
    extraOptions = '--cfg %s' % extraOptions

  arguments = {'Job': jobParams,
               'CE': resourceParams,
               'Optimizer': optimizerParams}
  log.verbose('Job arguments are: \n %s' % (arguments))

  siteRoot = gConfig.getValue('/LocalSite/Root', os.getcwd())
  log.debug('SiteRootPythonDir is:\n%s' % siteRoot)
  workingDir = gConfig.getValue('/LocalSite/WorkingDirectory', siteRoot)
  mkDir('%s/job/Wrapper' % (workingDir))

  diracRoot = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

  jobWrapperFile = '%s/job/Wrapper/Wrapper_%s' % (workingDir, jobID)
  if os.path.exists(jobWrapperFile):
    log.verbose('Removing existing Job Wrapper for %s' % (jobID))
    os.remove(jobWrapperFile)
  with open(os.path.join(diracRoot, defaultWrapperLocation), 'r') as fd:
    wrapperTemplate = fd.read()

  if 'LogLevel' in jobParams:
    logLevel = jobParams['LogLevel']
    log.info('Found Job LogLevel JDL parameter with value: %s' % (logLevel))
  else:
    log.info('Applying default LogLevel JDL parameter with value: %s' % (logLevel))

  dPython = sys.executable
  realPythonPath = os.path.realpath(dPython)
  log.debug('Real python path after resolving links is: ', realPythonPath)
  dPython = realPythonPath

  # Making real substitutions
  # wrapperTemplate = wrapperTemplate.replace( "@JOBARGS@", str( arguments ) )
  wrapperTemplate = wrapperTemplate.replace("@SITEPYTHON@", str(siteRoot))

  jobWrapperJsonFile = jobWrapperFile + '.json'
  with io.open(jobWrapperJsonFile, 'w', encoding='utf8') as jsonFile:
    json.dump(six.text_type(arguments), jsonFile, ensure_ascii=False)

  with open(jobWrapperFile, "w") as wrapper:
    wrapper.write(wrapperTemplate)

  jobExeFile = '%s/job/Wrapper/Job%s' % (workingDir, jobID)
  jobFileContents = \
      """#!/bin/sh
%s %s %s -o LogLevel=%s -o /DIRAC/Security/UseServerCertificate=no
""" % (dPython, jobWrapperFile, extraOptions, logLevel)
  with open(jobExeFile, 'w') as jobFile:
    jobFile.write(jobFileContents)

  return S_OK(jobExeFile)


def createRelocatedJobWrapper(wrapperPath, rootLocation,
                              jobID, jobParams, resourceParams, optimizerParams,
                              extraOptions='',
                              defaultWrapperLocation='DIRAC/WorkloadManagementSystem/JobWrapper/JobWrapperTemplate.py',
                              log=gLogger, logLevel='INFO'):
  """ This method creates a job wrapper for a specific job in wrapperPath,
      but assumes this has been reloated to rootLocation before running it.
  """
  if isinstance(extraOptions, six.string_types) and extraOptions.endswith('.cfg'):
    extraOptions = '--cfg %s' % extraOptions

  arguments = {'Job': jobParams,
               'CE': resourceParams,
               'Optimizer': optimizerParams}
  log.verbose('Job arguments are: \n %s' % (arguments))

  diracRoot = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

  jobWrapperFile = os.path.join(wrapperPath, 'Wrapper_%s' % jobID)
  if os.path.exists(jobWrapperFile):
    log.verbose('Removing existing Job Wrapper for %s' % (jobID))
    os.remove(jobWrapperFile)
  with open(os.path.join(diracRoot, defaultWrapperLocation), 'r') as fd:
    wrapperTemplate = fd.read()

  if 'LogLevel' in jobParams:
    logLevel = jobParams['LogLevel']
    log.info('Found Job LogLevel JDL parameter with value: %s' % (logLevel))
  else:
    log.info('Applying default LogLevel JDL parameter with value: %s' % (logLevel))

  # Making real substitutions
  # wrapperTemplate = wrapperTemplate.replace( "@JOBARGS@", str( arguments ) )
  wrapperTemplate = wrapperTemplate.replace("@SITEPYTHON@", rootLocation)

  jobWrapperJsonFile = jobWrapperFile + '.json'
  with io.open(jobWrapperJsonFile, 'w', encoding='utf8') as jsonFile:
    json.dump(six.text_type(arguments), jsonFile, ensure_ascii=False)

  with open(jobWrapperFile, "w") as wrapper:
    wrapper.write(wrapperTemplate)

  # The "real" location of the jobwrapper after it is started
  jobWrapperDirect = os.path.join(rootLocation, 'Wrapper_%s' % jobID)
  jobExeFile = os.path.join(wrapperPath, 'Job%s' % jobID)
  jobFileContents = \
      """#!/bin/sh
python %s %s -o LogLevel=%s -o /DIRAC/Security/UseServerCertificate=no
""" % (jobWrapperDirect, extraOptions, logLevel)
  with open(jobExeFile, 'w') as jobFile:
    jobFile.write(jobFileContents)

  jobExeDirect = os.path.join(rootLocation, 'Job%s' % jobID)
  return S_OK(jobExeDirect)
