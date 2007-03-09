# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/LoggingSystem/test/test_Logger.py,v 1.1 2007/03/09 15:45:53 rgracian Exp $
__RCSID__ = "$Id: test_Logger.py,v 1.1 2007/03/09 15:45:53 rgracian Exp $"
"""
"""
from dirac import DIRAC

try:
  for i in None:
    pass
except:
  pass

# for i in range(100):
#   DIRAC.gLogger.info( "test %s" % i )

testList = [{ 'method'    : DIRAC.gLogger.always,
              'arguments' : ( ( "This is a always message" ), ),
              'output'    : True
            },
            { 'method'    : DIRAC.gLogger.vital,
              'arguments' : ( ( "This is a vital message" ), ),
              'output'    : True
            },
            { 'method'    : DIRAC.gLogger.info,
              'arguments' : ( ( "This is a info message" ), ),
              'output'    : True
            },
            { 'method'    : DIRAC.gLogger.debug,
              'arguments' : ( ( "This is a debug message" ), ),
              'output'    : True
            },
            { 'method'    : DIRAC.gLogger.warn,
              'arguments' : ( ( "This is a warn message" ), ),
              'output'    : True
            },
            { 'method'    : DIRAC.gLogger.error,
              'arguments' : ( ( "This is a error message" ), ),
              'output'    : True
            },
            { 'method'    : DIRAC.gLogger.exception,
              'arguments' : ( ( "This is a exception message" ), ),
              'output'    : True
            },
            { 'method'    : DIRAC.gLogger.fatal,
              'arguments' : ( ( "This is a fatal message" ), ),
              'output'    : True
            },
            { 'method'    : DIRAC.gLogger.setName,
              'arguments' : ( "test_Logger" , ),
              'output'    : "test_Logger"
            },
            ]
testdict = { 'DIRAC.gLogger'               : testList,}

def runTests( testDict, name=None ):

  if name: print "Running tests for %s ..." % name
  for module in testDict:
    testFailed = False
    # print module, testDict[module]
    for test in testDict[module]:
        # print "Running Test %s" % module
        retVal = apply( test['method'], test['arguments'] )
        if retVal <> test['output']:
          print "Test for %s Failed" % module
          print "Error %s%s should return '%s', " % ( test['method'].__name__, test['arguments'], test['output'])
          print "It returns '%s'" % retVal, type( retVal )
          print "Failed"
          testFailed = True
          break
    if testFailed:
      DIRAC.sys.exit(-1)
    print "Test for %s OK" % module

  if name : print "Tests for %s OK" % name
  return True

  
runTests( testdict, 'DIRAC.Information.Logger' )

DIRAC.exit()
