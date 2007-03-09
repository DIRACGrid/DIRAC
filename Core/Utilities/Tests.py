# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/Utilities/Tests.py,v 1.1 2007/03/09 15:33:19 rgracian Exp $
__RCSID__ = "$Id: Tests.py,v 1.1 2007/03/09 15:33:19 rgracian Exp $"
'''
   DIRAC Utility module to run tests
'''

def run( testDict, name=None ):
  import DIRAC

  if name: DIRAC.gLogger.info( "Running tests", "for %s ..." % name )
  for module in testDict:
    testFailed = False
    for test in testDict[module]:
        retVal = apply( test['method'], test['arguments'] )
        if retVal <> test['output']:
          DIRAC.gLogger.error( "Failed test:", "%s\n" % module +
                               "%s%s should return '%s', " % ( test['method'].__name__, 
                                                               test['arguments'], 
                                                               test['output'] ) +
                               "It returns '%s'" % retVal, type( retVal ) )
          testFailed = True
          break
    if testFailed:
      DIRAC.gLogger.exception('')
      DIRAC.sys.exit(-1)
    DIRAC.gLogger.info( "Test OK", "for module %s" % module )

  if name : DIRAC.gLogger.info( "Tests OK", "for %s" % name )

  return True
