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
import math, sys, pprint, os

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

outputFile = "output.txt"
def setOutputFile( value ):
  global outputFile
  outputFile = value
  return S_OK()

catalog = 'AugerTestFileCatalog'
def setCatalog( value ):
  global catalog
  catalog = value
  return S_OK()

fullTest = False
def setFullTest( value ):
  global fullTest
  fullTest = True
  return S_OK()

Script.registerSwitch( "t:", "type=", "test type", setTestType )
Script.registerSwitch( "D:", "directory=", "test directory", setTestDirectory )
Script.registerSwitch( "N:", "clients=", "number of parallel clients", setNumberOfClients )
Script.registerSwitch( "Q:", "queries=", "number of queries in one test", setNumberOfQueries )
Script.registerSwitch( "T:", "tests=", "number of tests in a series", setNumberOfTests )
Script.registerSwitch( "C:", "catalog=", "catalog to use", setCatalog )
Script.registerSwitch( "L:", "lfnList=", "file with a list of LFNs", setLFNListFile )
Script.registerSwitch( "F", "fullTest", "run the full test", setFullTest )
Script.registerSwitch( "O:", "output=", "file with output result", setOutputFile )

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

    #if i % 50 == 0:
    #  print '\033[1D' + ".",

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

  global nClients, nQueries, testType, resultTest

  resultTest = []

  pp = ProcessPool( nClients )

  testFunction = eval( testType )

  for c in range( nClients ):
    pp.createAndQueueTask( testFunction, [nQueries],
                           callback=finalize,
                           exceptionCallback=doException )

  pp.processAllResults(3600)
  pp.finalize(0)

  average = 0.

  #print "\nTest %d results:" % testCount

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
    #print "Average time %.2f sec" % average
    #print "Average response time %.2f sec" % (average/nQueries)
    #print "Average query rate %.2f Hz" % (nQueries*nClients/average)
    if failure:
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

def runTests():

  global nTests, nClients, lfnListFile, testDir

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

  if testDir:
    print "\nTest results for clients %d, %s" % ( nClients, testDir )
  else:
    print "\nTest results for clients %d, %s" % ( nClients, lfnListFile )
  a1,s1 = doStats( testTimes )
  print "Test time: %.2f +/- %.2f" % (a1,s1)
  a2,s2 = doStats( queryTimes )
  print "Query time: %.2f +/- %.2f" % (a2,s2)
  a3,s3 = doStats( queryRates )
  print "Query rate: %.2f +/- %.2f" % (a3,s3)

  return( (a1,s1), (a2,s2), (a3,s3) )

numberOfFilesList = [ 10, 100, 500, 1000, 2000, 5000, 10000, 15000, 20000 ]
numberOfClientsList = [1,2,3,5,7,10,12,15,20]
directoriesList = [ (35455, "/auger/prod/QGSjetII_gr20_simADSTv2r5p1/en18.000/th0.65/2008/11/12"),
                    (24024, "/auger/prod/QGSjetII_gr20/2008/09/04/en17.500/th0.65"),
                    (15205, "/auger/generated/2012-09-03"),
                    (9907, "/auger/prod/QGSjetII_gr20/2008/09/03/en17.500/th0.65"),
                    (5157, "/auger/prod/QGSjetII_gr20/2008/09/04/en20.000/th0.65"),
                    (3338, "/auger/prod/QGSjetII_gr20/2008/09/05/en17.500/th0.65"),
                    (2538, "/auger/prod/QGSjetII_gr21/2009/01/12/en18.500/th0.65"),
                    (1500, "/auger/prod/epos_gr03_sim/en17.500/th26.000"),
                    (1000, "/auger/prod/PhotonLib_gr22/2009/02/27/en17.500/th26.000"),
                    (502, "/auger/prod/REPLICATED20081014/epos_gr08/en21.250/th26.000")
                  ]

def executeTest( nc, nf, queryDict, rateDict, queryDict_r, rateDict_r ):

  t1,t2,t3 = runTests()

  query,querys = t2
  rate, rates = t3

  fileLabel = "%d files" % nf
  queryDict.setdefault( fileLabel, {} )
  queryDict[fileLabel][nc] = query
  rateDict.setdefault( fileLabel, {} )
  rateDict[fileLabel][nc] = rate

  clientLabel = "%d clients" % nc
  queryDict_r.setdefault( clientLabel, {} )
  queryDict_r[clientLabel][nf] = query
  rateDict_r.setdefault( clientLabel, {} )
  rateDict_r[clientLabel][nf] = rate


def runFullTest():

  global outputFile, nClients, testDir

  queryDict = {}
  rateDict = {}

  queryDict_r = {}
  rateDict_r = {}

  for nc in numberOfClientsList:
    if testType in ['getBulkReplicas']:
      for nf in numberOfFilesList:
        lfnListFile = "lfns_%d.txt" % nf
        executeTest( nc, nf, queryDict, rateDict, queryDict_r, rateDict_r )
    elif testType in ['getDirectoryReplicas', "listDirectory"]:
      for nf, directory in directoriesList:
        testDir = directory
        executeTest( nc, nf, queryDict, rateDict, queryDict_r, rateDict_r )

  # Writing out result
  outFile = open( outputFile, "w" )
  outFile.write( "Test type %s \n" % testType )
  outFile.write( "Number of queries per unit test %d \n" % nQueries )
  outFile.write( "Number of tests in a series %d \n" % nTests )
  outFile.write( "Results: \n\n\n" )
  outFile.write( str( queryDict ) + '\n\n\n' )
  outFile.write( str( rateDict ) + '\n\n\n' )
  outFile.write( str( queryDict_r ) + '\n\n\n' )
  outFile.write( str( rateDict_r ) + '\n\n\n' )
  outFile.close()

  pprint.pprint( queryDict )
  pprint.pprint( rateDict )
  pprint.pprint( queryDict_r )
  pprint.pprint( rateDict_r )

#########################################################################

if os.path.exists( outputFile ):
  print "Output file %s already exists, exiting ..."
  sys.exit(-1)

if fullTest:
  runFullTest()
else:
  runTests()
