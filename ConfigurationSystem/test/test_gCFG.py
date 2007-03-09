# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/ConfigurationSystem/test/test_gCFG.py,v 1.1 2007/03/09 15:22:42 rgracian Exp $
__RCSID__ = "$Id: test_gCFG.py,v 1.1 2007/03/09 15:22:42 rgracian Exp $"

from dirac import DIRAC

DIRAC.gLogger.setName('test_gConfig')

# DIRAC.gConfig.loadFile('test.cfg')
# import os

# print os.path.abspath( os.curdir )

testList = [{ 'method'    : DIRAC.gConfig.getOption,
              'arguments' : ( '/testSection/test', ),
              'output'    : True
            },             
             ]

testdict = { 'DIRAC.gConfig'               : testList,}

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
      DIRAC.gLogger.exception('')
      DIRAC.sys.exit(-1)
    print "Test for %s OK" % module

  if name : print "Tests for %s OK" % name
  return True

  
runTests( testdict, 'DIRAC.Information.Logger' )


DIRAC.exit()
