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
def setTestType( value ):
  global testType 
  testType = value
  return S_OK()

testDir = ''
def setTestDirectory( value ):
  global testDir
  testDir = value
  return S_OK()  

nClients = 1
def setNumberOfClients( value ):
  global nClients
  nClients = int( value )
  return S_OK()  

nQueries = 100  
def setNumberOfQueries( value ):
  global nQueries
  nQueries = int( value )
  return S_OK() 

catalog = 'AugerTestFileCatalog'
def setCatalog( value ):
  global catalog
  catalog = value
  return S_OK()   

Script.registerSwitch( "t:", "type=", "test type", setTestType )
Script.registerSwitch( "D:", "directory=", "test directory", setTestDirectory )
Script.registerSwitch( "N:", "clients=", "number of parallel clients", setNumberOfClients )
Script.registerSwitch( "Q:", "queries=", "number of queries", setNumberOfQueries )
Script.registerSwitch( "C:", "catalog=", "catalog to use", setCatalog )


Script.parseCommandLine( ignoreErrors = True )

from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
from DIRAC.Core.Utilities.ProcessPool import ProcessPool

from DIRAC import S_OK
import time

fc = FileCatalog( catalogs=[catalog] )

lfnList = []

def listDirectory( n_queries, lfn_list ):

  start = time.time()
  sCount = 0
  fCount = 0
  for i in range( n_queries ) :
    result = fc.listDirectory('/auger/prod/ComPhotonQGSjetII_gr139/en16.500/th0.65/048279')
    if result['OK']:
      sCount += 1
    else:
      fCount += 1

    if i % 50 == 0:
      print '\033[1D' + ".",

  total = time.time() - start

  #print "Total", total, 'Success', sCount, 'Failure', fCount

  result = S_OK( (total,sCount,fCount) )
  return result

resultTest = []

def getBulkReplicas( n_queries, lfn_list ):

  start = time.time()
  sCount = 0
  fCount = 0
  for i in range( n_queries ) :
    result = fc.getReplicas( lfnList )
    if result['OK']:
      sCount += 1
    else:
      fCount += 1

    if i % 50 == 0:
      print '\033[1D' + ".",

  total = time.time() - start

  #print "Total", total, 'Success', sCount, 'Failure', fCount

  result = S_OK( (total,sCount,fCount) )
  return result


def finalize(task,result):

  global resultTest 

  #if result['OK']:
  #  print "Test time ", result['Value'], task.getTaskID()
  #else:
  #  print "Error:", result['Message']

  resultTest.append( result['Value'] ) 

def doException( expt ):
  print "Exception", expt


pp = ProcessPool( nClients )

testFunction = eval( testType )

for c in range( nClients ):
  pp.createAndQueueTask( testFunction, [nQueries,lfnList], 
                         callback=finalize, 
                         exceptionCallback=doException )

pp.processAllResults(3600)
pp.finalize()

average = 0.

print "\nFinal results:"

sum = 0.
count = 0
for testTime,success,failure in resultTest:
  #print testTime,success,failure
  count += 1
  sum += testTime

if count != 0:
  average = sum/count
  print "Average time %.2f sec" % average
  print "Average response time %.2f sec" % (average/nQueries)
  print "Average query rate %.2f Hz" % (nQueries*nClients/average)   
else:
  print "Test failed", sum,count 