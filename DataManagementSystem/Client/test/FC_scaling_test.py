# TO-DO: to be moved to TestDIRAC

########################################################################
# $HeadURL$
# File :    FC_Scaling_test
# Author :  Andrei Tsaregorodtsev
########################################################################
"""
  Test suite for a generic File Catalog scalability tests
"""
__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script
from DIRAC import S_OK
import sys, pprint, os, numpy

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

shortRange = False
def setShortRange( value ):
  global shortRange
  shortRange = True
  return S_OK()

verbosity = 0
def setVerbosity( value ):
  global verbosity
  verbosity += 1
  return S_OK()

Script.registerSwitch( "t:", "type=", "test type", setTestType )
Script.registerSwitch( "D:", "directory=", "test directory", setTestDirectory )
Script.registerSwitch( "N:", "clients=", "number of parallel clients", setNumberOfClients )
Script.registerSwitch( "Q:", "queries=", "number of queries in one test", setNumberOfQueries )
Script.registerSwitch( "C:", "catalog=", "catalog to use", setCatalog )
Script.registerSwitch( "L:", "lfnList=", "file with a list of LFNs", setLFNListFile )
Script.registerSwitch( "F", "fullTest", "run the full test", setFullTest )
Script.registerSwitch( "O:", "output=", "file with output result", setOutputFile )
Script.registerSwitch( "v", "verbose", "file with output result", setVerbosity )
Script.registerSwitch( "S", "shortRange", "run short parameter range", setShortRange )

Script.parseCommandLine( ignoreErrors = True )

from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
from DIRAC.Core.Utilities.ProcessPool import ProcessPool

from DIRAC import S_OK
import time

fc = FileCatalog( catalogs=[catalog] )

resultTest = []

def listDirectory( n_queries ):

  global testDir

  start = time.time()
  sCount = 0
  fCount = 0
  resultList = []

  startTotal = time.time()

  for i in range( n_queries ) :

    start = time.time()
    result = fc.listDirectory( testDir )
    resultList.append( time.time() - start )

    if result['OK']:
      sCount += 1
    else:
      fCount += 1

  total = time.time() - startTotal

  average, error = doStats( resultList )

  if verbosity >= 1:
    print "getReplicas: Total time", total, 'Success', sCount, 'Failure', \
                                     fCount, 'Average', average, 'Stdvar', error

  result = S_OK( (resultList, sCount, fCount) )
  return result

def getBulkReplicas( n_queries ):

  global lfnListFile, verbosity
  lFile = open(lfnListFile)
  lfnList = [ l.strip().replace('//','/') for l in lFile.read().strip().split() ]
  lFile.close()

  start = time.time()
  sCount = 0
  fCount = 0
  resultList = []

  startTotal = time.time()

  for i in range( n_queries ) :

    start = time.time()
    result = fc.getReplicas( lfnList )
    resultList.append( time.time() - start )

    if verbosity >= 2:
      print "getReplicas: received lfns", len(result['Value']['Successful'])
      for lfn in result['Value']['Successful']:
        print result['Value']['Successful'][lfn]
        if verbosity >= 3:
          for lfn,res in result['Value']['Successful'].items():
            print lfn
            print res
            break

    if result['OK']:
      sCount += 1
    else:
      fCount += 1

  total = time.time() - startTotal

  average, error = doStats( resultList )

  if verbosity >= 1:
    print "getReplicas: Total time", total, 'Success', sCount, 'Failure', \
                                     fCount, 'Average', average, 'Stdvar', error

  result = S_OK( (resultList, sCount, fCount) )
  return result

def getDirectoryReplicas( n_queries ):

  global testDir, verbosity

  sCount = 0
  fCount = 0
  resultList = []

  startTotal = time.time()

  for i in range( n_queries ) :

    start = time.time()
    result = fc.getDirectoryReplicas( testDir )
    resultList.append( time.time() - start )

    if verbosity >= 2:
      print "Returned values", len(result['Value']['Successful'][testDir])
      for lfn,res in result['Value']['Successful'][testDir].items():
        print lfn
        print res
        break

    if result['OK']:
      sCount += 1
    else:
      fCount += 1

  total = time.time() - startTotal

  average, error = doStats( resultList )

  if verbosity >= 1:
    print "getDirectoryReplicas: Total time", total, 'Success', sCount, 'Failure', \
                                              fCount, '\nAverage', average, 'Stdvar', error

  result = S_OK( (resultList, sCount, fCount) )
  return result

def finalize(task,result):

  global resultTest, verbosity

  if verbosity >= 2:
    if result['OK']:
      print "Test time ", result['Value'], task.getTaskID()
    else:
      print "Error:", result['Message']

  resultTest.append( result['Value'] )

def doException( expt ):
  print "Exception", expt

def runTest( ):

  global nClients, nQueries, testType, resultTest, testDir, lfnListFile

  resultTest = []

  pp = ProcessPool( nClients )

  testFunction = eval( testType )

  for c in range( nClients ):
    pp.createAndQueueTask( testFunction, [nQueries],
                           callback=finalize,
                           exceptionCallback=doException )

  pp.processAllResults(3600)
  pp.finalize(0)

  timeResult = []
  for testTime,success,failure in resultTest:
    #print testTime,success,failure
    timeResult += testTime

  averageTime, errorTime = doStats( timeResult )
  rateResult = [ nClients/t for t in timeResult ]
  averageRate, errorRate = doStats( rateResult )

  if testDir:
    print "\nTest results for clients %d, %s" % ( nClients, testDir )
  else:
    print "\nTest results for clients %d, %s" % ( nClients, lfnListFile )

  print "Query time: %.2f +/- %.2f" % (averageTime, errorTime)
  print "Query rate: %.2f +/- %.2f" % (averageRate, errorRate)

  return( (averageTime, errorTime), (averageRate, errorRate) )

def doStats( testArray ):

  array = list( testArray )
  # Delete min and max value first
  del array[ array.index(max(array)) ]
  del array[ array.index(min(array)) ]

  numArray = numpy.array( array )
  average = numpy.mean( numArray )
  stddev = numpy.std( numArray )

  return (average, stddev)

numberOfFilesList = [ 10, 100, 500, 1000, 2000, 5000, 10000, 15000, 20000 ]
numberOfFilesList_short = [ 100, 1000, 5000, 10000, 20000 ]
numberOfClientsList = [1,2,3,5,7,10,12,15,20,30,50,75]
numberOfClientsList_short = [1,5,10,20]
directoriesList = [ (35455, "/auger/prod/QGSjetII_gr20_simADSTv2r5p1/en18.000/th0.65/2008/11/12"),
                    (24024, "/auger/prod/QGSjetII_gr20/2008/09/04/en17.500/th0.65"),
                    #(15205, "/auger/generated/2012-09-03"),
                    (18391,"/auger/prod/QGSjetII_gr20_simADSTv2r5p1/en17.500/th0.65/2008/11/11"),
                    (9907, "/auger/prod/QGSjetII_gr20/2008/09/03/en17.500/th0.65"),
                    (5157, "/auger/prod/QGSjetII_gr20/2008/09/04/en20.000/th0.65"),
                    (2538, "/auger/prod/QGSjetII_gr21/2009/01/12/en18.500/th0.65"),
                    (1500, "/auger/prod/epos_gr03_sim/en17.500/th26.000"),
                    (502, "/auger/prod/REPLICATED20081014/epos_gr08/en21.250/th26.000")
                  ]
directoriesList_short = [ (35455, "/auger/prod/QGSjetII_gr20_simADSTv2r5p1/en18.000/th0.65/2008/11/12"),
                          (18391,"/auger/prod/QGSjetII_gr20_simADSTv2r5p1/en17.500/th0.65/2008/11/11"),
                          (5157, "/auger/prod/QGSjetII_gr20/2008/09/04/en20.000/th0.65"),
                          (1000, "/auger/prod/PhotonLib_gr22/2009/02/27/en17.500/th26.000")
                  ]
directoriesList.reverse()
directoriesList_short.reverse()

def executeTest( nc, nf, queryDict, rateDict, queryDict_r, rateDict_r ):

  global nClients

  nClients = nc

  t1,t2 = runTest()

  query,querys = t1
  rate, rates = t2

  fileLabel = "%d files" % nf
  queryDict.setdefault( fileLabel, {} )
  queryDict[fileLabel][nc] = (query,querys)
  rateDict.setdefault( fileLabel, {} )
  rateDict[fileLabel][nc] = (rate,rates)

  clientLabel = "%d clients" % nc
  queryDict_r.setdefault( clientLabel, {} )
  queryDict_r[clientLabel][nf] = (query,querys)
  rateDict_r.setdefault( clientLabel, {} )
  rateDict_r[clientLabel][nf] = (rate,rates)


def runFullTest():

  global outputFile, nClients, testDir, lfnListFile, shortRange

  queryDict = {}
  rateDict = {}

  queryDict_r = {}
  rateDict_r = {}

  ncList = numberOfClientsList
  if shortRange:
    ncList = numberOfClientsList_short

  nfList = numberOfFilesList
  if shortRange:
    nfList = numberOfFilesList_short

  ndList = directoriesList
  if shortRange:
    ndList = directoriesList_short

  for nc in ncList:
    if testType in ['getBulkReplicas']:
      for nf in nfList:
        lfnListFile = "lfns_%d.txt" % nf
        executeTest( nc, nf, queryDict, rateDict, queryDict_r, rateDict_r )
    elif testType in ['getDirectoryReplicas', "listDirectory"]:
      for nf, directory in ndList:
        testDir = directory
        executeTest( nc, nf, queryDict, rateDict, queryDict_r, rateDict_r )

  # Writing out result
  outFile = open( outputFile, "w" )
  outFile.write( "Test type %s \n" % testType )
  outFile.write( "Number of queries per unit test %d \n" % nQueries )
  outFile.write( "Results: \n\n\n" )
  outFile.write( 'data_f = ' + str( queryDict ) + '\n\n\n' )
  outFile.write( 'data_f_r = ' + str( rateDict ) + '\n\n\n' )
  outFile.write( 'data_c = ' + str( queryDict_r ) + '\n\n\n' )
  outFile.write( 'data_c_r = ' + str( rateDict_r ) + '\n\n\n' )
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
  runTest()
