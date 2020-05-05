""" This script submits a test prodJobuction with filter
"""

# pylint: disable=wrong-import-position, protected-access

from __future__ import print_function
from __future__ import absolute_import
from __future__ import division
import time
import os

import json
from DIRAC.Core.Base import Script
Script.setUsageMessage('\n'.join([__doc__.split('\n')[1],
                                  'Usage:',
                                  '  %s test directory' % Script.scriptName
                                  ]))

Script.registerSwitch("", "UseFilter=", "e.g. True/False")
Script.parseCommandLine()

from DIRAC import gLogger
from DIRAC.Interfaces.API.Job import Job
from DIRAC.TransformationSystem.Client.Transformation import Transformation
# Needed to test transformations with Filters
from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
from DIRAC.DataManagementSystem.Client.DataManager import DataManager

# Parse the arguments
args = Script.getPositionalArgs()
if len(args) != 1:
  Script.showHelp()
directory = args[0]

UseFilter = None
for switch, switchValue in Script.getUnprocessedSwitches():
  if switch == "UseFilter":
      UseFilter = True if switchValue.lower() == "true" else False

# Let's first create the prodJobuction
prodJobType = 'Merge'
transName = 'testProduction_' + str(int(time.time()))
desc = 'just test'

prodJob = Job()
prodJob._addParameter(prodJob.workflow, 'PRODUCTION_ID', 'string', '00012345', 'ProductionID')
prodJob._addParameter(prodJob.workflow, 'JOB_ID', 'string', '00006789', 'ProductionJobID')
prodJob._addParameter(prodJob.workflow, 'eventType', 'string', 'TestEventType', 'Event Type of the prodJobuction')
prodJob._addParameter(prodJob.workflow, 'numberOfEvents', 'string', '-1', 'Number of events requested')
prodJob._addParameter(prodJob.workflow, 'ProcessingType', 'JDL', str('Test'), 'ProductionGroupOrType')
prodJob._addParameter(prodJob.workflow, 'Priority', 'JDL', str(9), 'UserPriority')
prodJob.setType(prodJobType)
prodJob.workflow.setName(transName)
prodJob.workflow.setDescrShort(desc)
prodJob.workflow.setDescription(desc)
prodJob.setCPUTime(86400)
prodJob.setInputDataPolicy('Download')
prodJob.setExecutable('/bin/ls', '-l', modulesList=['Script', 'FailoverRequest'])

# Let's submit the prodJobuction now
# result = prodJob.create()

name = prodJob.workflow.getName()
name = name.replace('/', '').replace('\\', '')
prodJob.workflow.toXMLFile(name)

print('Workflow XML file name is: %s' % name)

workflowBody = ''
if os.path.exists(name):
  with open(name, 'r') as fopen:
    workflowBody = fopen.read()
else:
  print('Could not get workflow body')

# Standard parameters
transformation = Transformation()
transformation.setTransformationName(name)
transformation.setTransformationGroup('Test')
transformation.setDescription(desc)
transformation.setLongDescription(desc)
transformation.setType('Merge')
transformation.setBody(workflowBody)
transformation.setPlugin('Standard')
transformation.setTransformationFamily('Test')
transformation.setGroupSize(2)
transformation.setOutputDirectories(['/dirac/outConfigName/configVersion/LOG/00000000',
                                     '/dirac/outConfigName/configVersion/RAW/00000000',
                                     '/dirac/outConfigName/configVersion/CORE/00000000'])


# Set directory meta data and create a transformation with a meta-data filter
if UseFilter:
  fc = FileCatalog()
  dm = DataManager()
  metaCatalog = 'DIRACFileCatalog'

  # Set meta data fields in the DFC
  MDFieldDict = {'particle': 'VARCHAR(128)', 'timestamp': 'VARCHAR(128)'}
  for MDField in MDFieldDict.keys():
    MDFieldType = MDFieldDict[MDField]
    res = fc.addMetadataField(MDField, MDFieldType)
    if not res['OK']:
      gLogger.error("Failed to add metadata fields", res['Message'])
      exit(-1)

  # Set directory meta data
  timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
  MDdict1 = {'particle': 'gamma', 'timestamp': timestamp}
  res = fc.setMetadata(directory, MDdict1)
  if not res['OK']:
    gLogger.error("Failed to set metadata", res['Message'])
    exit(-1)

  # Set the transformation meta data filter
  MDdict1b = {'particle': 'gamma', 'timestamp': timestamp}
  mqJson1b = json.dumps(MDdict1b)
  res = transformation.setInputMetaQuery(MDdict1b)
  if not res['OK']:
    gLogger.error("Failed to set InputMetaQuery", res['Message'])
    exit(-1)

# Create the transformation
result = transformation.addTransformation()

if not result['OK']:
  print(result)
  exit(1)

transID = result['Value']
with open('TransformationID', 'w') as fd:
  fd.write(str(transID))
print("Created %s, stored in file 'TransformationID'" % transID)
