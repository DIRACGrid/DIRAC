""" This script submits a test prodJobuction
"""

import time
import os

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC.Interfaces.API.Job import Job
from DIRAC.TransformationSystem.Client.Transformation import Transformation
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient

#Let's first create the prodJobuction
prodJobType = 'Merge'
transName = 'testProduction_'  + str(int(time.time()))
desc = 'just test'

prodJob = Job()
prodJob._addParameter( prodJob.workflow, 'eventType', 'string', 'TestEventType', 'Event Type of the prodJobuction' )
prodJob._addParameter( prodJob.workflow, 'numberOfEvents', 'string', '-1', 'Number of events requested' )
prodJob._addParameter( prodJob.workflow, 'ProcessingType', 'JDL', str( 'Test' ), 'ProductionGroupOrType' )
prodJob._addParameter( prodJob.workflow, 'Priority', 'JDL', str( 9 ), 'UserPriority' )
prodJob.setType( prodJobType )
prodJob.workflow.setName(transName)
prodJob.workflow.setDescrShort( desc )
prodJob.workflow.setDescription( desc )
prodJob.setCPUTime( 86400 )
prodJob.setInputDataPolicy( 'Download' )
prodJob.setExecutable('/bin/ls', '-l')

#Let's submit the prodJobuction now
#result = prodJob.create()

name = prodJob.workflow.getName()
name = name.replace( '/', '' ).replace( '\\', '' )
prodJob.workflow.toXMLFile( name )

print 'Workflow XML file name is: %s' % name

workflowBody = ''
if os.path.exists( name ):
  with open( name, 'r' ) as fopen:
    workflowBody = fopen.read()
else:
  print 'Could not get workflow body'

# Standard parameters
transformation = Transformation()
transformation.setTransformationName( name )
transformation.setTransformationGroup( 'Test' )
transformation.setDescription( desc )
transformation.setLongDescription( desc )
transformation.setType( 'Merge' )
transformation.setBody( workflowBody )
transformation.setPlugin( 'Standard' )
transformation.setTransformationFamily( 'Test' )
transformation.setGroupSize( 2 )
transformation.setOutputDirectories([ '/dirac/outConfigName/configVersion/LOG/00000000',
                                      '/dirac/outConfigName/configVersion/RAW/00000000',
                                      '/dirac/outConfigName/configVersion/CORE/00000000'])

result = transformation.addTransformation()
if not result['OK']:
  print result
  exit(1)

transID = result['Value']
with open('TransformationID', 'w') as fd:
  fd.write(str(transID))
print "Created %s, stored in file 'TransformationID'" % transID
