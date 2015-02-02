# $HeadURL$
'''
   DIRAC Utility module to run tests
'''
__RCSID__ = "$Id$"

def run( testDict, name = None ):
  import DIRAC

  if name:
    DIRAC.gLogger.always( "Running tests", "for %s ..." % name )
  for module in testDict:
    testFailed = False
    for test in testDict[module]:
      retVal = apply( test['method'], test['arguments'] )
      if retVal != test['output']:
        DIRAC.gLogger.error( "Failed test:", "%s\n" % module +
                             "%s%s should return '%s'\n" % ( test['method'].__name__,
                                                             test['arguments'],
                                                             test['output'] ) +
                             "It returns '%s' %s " % ( retVal, type( retVal ) ) )
        testFailed = True
        break
    if testFailed:
      DIRAC.gLogger.exception( '' )
      DIRAC.sys.exit( -1 )
    DIRAC.gLogger.info( "Test OK", "for module %s" % module )

  if name:
    DIRAC.gLogger.info( "Tests OK", "for %s" % name )

  return True
