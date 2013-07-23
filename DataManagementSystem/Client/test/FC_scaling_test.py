# $HeadURL$
# File :    FC_Scaling_test
# Author :  Andrei Tsaregorodtsev
########################################################################
"""
  Test suite for a generic File Catalog scalability tests
"""
__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script
from DIRAC import S_OK, S_ERROR

Script.setUsageMessage( """
  Test suite for a generic File Catalog scalability tests
"""                        )

testType = 'noTest'
def setTestType():
  global testType 
  testType = 'listDir'
  return S_OK()

Script.registerSwitch( "t:", "type=", "test type", setTestType )


Script.parseCommandLine( ignoreErrors = True )



from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
from DIRAC.Core.Utilities.ProcessPool import ProcessPool

from DIRAC import S_OK
import time

fc = FileCatalog( catalogs=['AugerTestFileCatalog'] )

def test():

  start = time.time()
  for i in range(200) :
    result = fc.listDirectory('/auger/prod/ComPhotonQGSjetII_gr139/en16.500/th0.65/048279')
    #if result['OK']:
    #  print '.',
    #else:
    #  print '-',

    if i % 100 == 0:
      print i

  total = time.time() - start

  print "Total", total

  result = S_OK(total)
  #print result
  return result

def finalize(task,result):

  if result['OK']:
    print "Test time ", result['Value'], task
  else:
    print "Error:", result['Message']

def doException( expt ):
  print "Exception", expt


pp = ProcessPool( 10 )

for c in range( 10  ):
  pp.createAndQueueTask( test, [], callback=finalize, exceptionCallback=doException )

pp.processAllResults()
