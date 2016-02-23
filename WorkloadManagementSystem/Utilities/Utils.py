""" Utilities for WMS
"""

import io
import os
import sys
import json

from DIRAC import gConfig, gLogger, S_OK, S_ERROR

def createJobWrapper( jobID, jobParams, resourceParams, optimizerParams,
                      extraOptions = '',
                      defaultWrapperLocation = 'DIRAC/WorkloadManagementSystem/JobWrapper/JobWrapperTemplate.py',
                      log = gLogger, logLevel = 'INFO' ):
  """ This method creates a job wrapper filled with the CE and Job parameters to execute the job.
      Main user is the JobAgent
  """

  arguments = {'Job':jobParams,
               'CE':resourceParams,
               'Optimizer':optimizerParams}
  log.verbose( 'Job arguments are: \n %s' % ( arguments ) )

  siteRoot = gConfig.getValue( '/LocalSite/Root', os.getcwd() )
  log.debug( 'SiteRootPythonDir is:\n%s' % siteRoot )
  workingDir = gConfig.getValue( '/LocalSite/WorkingDirectory', siteRoot )
  if not os.path.exists( '%s/job/Wrapper' % ( workingDir ) ):
    try:
      os.makedirs( '%s/job/Wrapper' % ( workingDir ) )
    except Exception:
      log.exception()
      return S_ERROR( 'Could not create directory for wrapper script' )

  diracRoot = os.path.dirname( os.path.dirname( os.path.dirname( os.path.dirname( __file__ ) ) ) )

  jobWrapperFile = '%s/job/Wrapper/Wrapper_%s' % ( workingDir, jobID )
  if os.path.exists( jobWrapperFile ):
    log.verbose( 'Removing existing Job Wrapper for %s' % ( jobID ) )
    os.remove( jobWrapperFile )
  with open( os.path.join( diracRoot, defaultWrapperLocation ), 'r' ) as fd:
    wrapperTemplate = fd.read()

  if jobParams.has_key( 'LogLevel' ):
    logLevel = jobParams['LogLevel']
    log.info( 'Found Job LogLevel JDL parameter with value: %s' % ( logLevel ) )
  else:
    log.info( 'Applying default LogLevel JDL parameter with value: %s' % ( logLevel ) )

  dPython = sys.executable
  realPythonPath = os.path.realpath( dPython )
  log.debug( 'Real python path after resolving links is: ', realPythonPath )
  dPython = realPythonPath

  # Making real substitutions
  # wrapperTemplate = wrapperTemplate.replace( "@JOBARGS@", str( arguments ) )
  wrapperTemplate = wrapperTemplate.replace( "@SITEPYTHON@", str( siteRoot ) )

  jobWrapperJsonFile = jobWrapperFile + '.json'
  with io.open( jobWrapperJsonFile, 'w', encoding = 'utf8' ) as jsonFile:
    json.dump( unicode(arguments), jsonFile, ensure_ascii=False )

  wrapper = open( jobWrapperFile, "w" )
  wrapper.write( wrapperTemplate )
  wrapper.close ()
  jobExeFile = '%s/job/Wrapper/Job%s' % ( workingDir, jobID )
  jobFileContents = \
"""#!/bin/sh
%s %s %s -o LogLevel=%s -o /DIRAC/Security/UseServerCertificate=no
""" % ( dPython, jobWrapperFile, extraOptions, logLevel )
  with open( jobExeFile, 'w' ) as jobFile:
    jobFile.write( jobFileContents )

  return S_OK( jobExeFile )
