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
import math

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

nTests = 1
def setNumberOfTests( value ):
  global nTests
  nTests = int( value )
  return S_OK()

lfnListFile = 'lfns_100.txt'
def setLFNListFile( value ):
  global lfnListFile
  lfnListFile = value
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
Script.registerSwitch( "T:", "tests=", "number of tests", setNumberOfTests )
Script.registerSwitch( "C:", "catalog=", "catalog to use", setCatalog )
Script.registerSwitch( "L:", "lfnList=", "file with a list of LFNs", setLFNListFile )


Script.parseCommandLine( ignoreErrors = True )

from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
from DIRAC.Core.Utilities.ProcessPool import ProcessPool

from DIRAC import S_OK
import time

fc = FileCatalog( catalogs=[catalog] )

def listDirectory( n_queries ):

  global testDir

  start = time.time()
  sCount = 0
  fCount = 0
  for i in range( n_queries ) :
    result = fc.listDirectory( testDir )
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

def getBulkReplicas( n_queries ):

  global lfnListFile
  lFile = open(lfnListFile)
  lfnList = [ l.strip().replace('//','/') for l in lFile.read().strip().split() ]
  lFile.close()

  start = time.time()
  sCount = 0
  fCount = 0
  for i in range( n_queries ) :

    result = fc.getReplicas( lfnList )

    #print "received lfns", len(result['Value']['Successful'])
    #for lfn in result['Value']['Successful']:
    #  print result['Value']['Successful'][lfn]
    #  break

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

def getDirectoryReplicas( n_queries ):

  global testDir

  start = time.time()
  sCount = 0
  fCount = 0
  for i in range( n_queries ) :
    result = fc.getDirectoryReplicas( testDir )

    #print "Returned values", len(result['Value']['Successful'][testDir])
    #for lfn,res in result['Value']['Successful'][testDir].items():
    #  print lfn
    #  print res
    #  break

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

def runTest( testCount ):

  global nClients, nQueries, testType

  pp = ProcessPool( nClients )

  testFunction = eval( testType )

  for c in range( nClients ):
    pp.createAndQueueTask( testFunction, [nQueries],
                           callback=finalize,
                           exceptionCallback=doException )

  pp.processAllResults(3600)
  pp.finalize(0)

  average = 0.

  print "\nTest %d results:" % testCount

  sum = 0.
  count = 0
  for testTime,success,failure in resultTest:
    #print testTime,success,failure
    count += 1
    sum += testTime

  if count != 0:
    average = sum/count
    avQuery = average/nQueries
    avQueryRate = nQueries*nClients/average
    print "Average time %.2f sec" % average
    print "Average response time %.2f sec" % (average/nQueries)
    print "Average query rate %.2f Hz" % (nQueries*nClients/average)
    print "Success rate %.2f" % ( float(success)/float(success+failure)*100. )
  else:
    print "Test failed", sum,count

  return average, avQuery, avQueryRate

def doStats( array ):

  # Delete min and max value first
  #del array[ array.index(max(array)) ]
  #del array[ array.index(min(array)) ]

  n = len(array)
  average = sum(array)/n
  devs = [ (entry - average)*(entry - average) for entry in array ]
  stddev = math.sqrt( sum(devs)/n )

  return average, stddev

testCount = 0
testTimes = []
queryTimes = []
queryRates = []

while testCount < nTests:

  testCount += 1

  testTime, query, queryRate = runTest( testCount )
  testTimes.append( testTime)
  queryTimes.append( query )
  queryRates.append( queryRate )


print "\nFinal results:"
a,s = doStats( testTimes )
print "Test time: %.2f +/- %.2f" % (a,s)
a,s = doStats( queryTimes )
print "Query time: %.2f +/- %.2f" % (a,s)
a,s = doStats( queryRates )
print "Query rate: %.2f +/- %.2f" % (a,s)

