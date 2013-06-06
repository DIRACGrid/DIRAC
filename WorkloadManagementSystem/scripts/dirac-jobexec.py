#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-jobexec
# Author :  Stuart Paterson
########################################################################
__RCSID__ = "$Id$"

""" The dirac-jobexec script is equipped to execute workflows that
    are specified via their XML description.  The main client of
    this script is the Job Wrapper.
"""

import DIRAC
from DIRAC.Core.Base import Script

# Register workflow parameter switch
Script.registerSwitch( 'p:', 'parameter=', 'Parameters that are passed directly to the workflow' )
Script.parseCommandLine()

from DIRAC.Core.Workflow.Parameter import *
from DIRAC.Core.Workflow.Module import *
from DIRAC.Core.Workflow.Step import *
from DIRAC.Core.Workflow.Workflow import *
from DIRAC.Core.Workflow.WorkflowReader import *
from DIRAC import S_OK, S_ERROR, gConfig, gLogger
from DIRAC.WorkloadManagementSystem.Client.JobReport import JobReport
from DIRAC.AccountingSystem.Client.DataStoreClient import DataStoreClient
from DIRAC.RequestManagementSystem.Client.Request import Request

import DIRAC

import os, os.path, sys, string

# Forcing the current directory to be the first in the PYTHONPATH
sys.path.insert( 0, os.path.realpath( '.' ) )
gLogger.showHeaders( True )

def jobexec( jobxml, wfParameters = {} ):
  jobfile = os.path.abspath( jobxml )
  if not os.path.exists( jobfile ):
    gLogger.warn( 'Path to specified workflow %s does not exist' % ( jobfile ) )
    sys.exit( 1 )
  workflow = fromXMLFile( jobfile )
  gLogger.debug( workflow )
  code = workflow.createCode()
  gLogger.debug( code )
  jobID = 0
  if os.environ.has_key( 'JOBID' ):
    jobID = os.environ['JOBID']
    gLogger.info( 'DIRAC JobID %s is running at site %s' % ( jobID, DIRAC.siteName() ) )

  workflow.addTool( 'JobReport', JobReport( jobID ) )
  workflow.addTool( 'AccountingReport', DataStoreClient() )
  workflow.addTool( 'Request', Request() )

  # Propagate the command line parameters to the workflow if any
  for name, value in wfParameters.items():
    workflow.setValue( name, value )

  result = workflow.execute()
  return result

positionalArgs = Script.getPositionalArgs()
if len( positionalArgs ) != 1:
  gLogger.debug( 'Positional arguments were %s' % ( positionalArgs ) )
  DIRAC.abort( 1, "Must specify the Job XML file description" )

if os.environ.has_key( 'JOBID' ):
  gLogger.info( 'JobID: %s' % ( os.environ['JOBID'] ) )

jobXMLfile = positionalArgs[0]
parList = Script.getUnprocessedSwitches()
parDict = {}
for switch, parameter in parList:
  if switch == "p":
    name, value = parameter.split( '=' )
    value = value.strip()

    # The comma separated list in curly brackets is interpreted as a list
    if value.startswith( "{" ):
      value = value[1:-1].replace( '"', '' ).replace( " ", '' ).split( ',' )
      value = ';'.join( value )

    parDict[name] = value

gLogger.debug( 'PYTHONPATH:\n%s' % ( string.join( sys.path, '\n' ) ) )
result = jobexec( jobXMLfile, parDict )
if not result['OK']:
  gLogger.debug( 'Workflow execution finished with errors, exiting' )
  sys.exit( 1 )
else:
  gLogger.debug( 'Workflow execution successful, exiting' )
  sys.exit( 0 )

