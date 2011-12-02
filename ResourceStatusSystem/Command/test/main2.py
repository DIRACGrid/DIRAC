import os
import pkgutil
#import sys
import unittest

'''
  ParseCommandLine stuff... just ignore it.
'''  
class WritableObject:
  def write(self,string):
    pass
#wo = WritableObject()
#sys.stderr = wo

from DIRAC.Core.Base import Script
Script.parseCommandLine()

separator = '-' * 70

'''
  Real code from here =)
'''

################################################################################
    
def loadTestComponent( testComponentName ):
  
  suite = False
      
  print '\n%s' % separator
  print '  %s ' % testComponentName
  print separator
  try:
    suite = __import__( testComponentName )
  except:
    pass   
    
  return suite  

################################################################################
    
def loadTestSuites( testComponent ):
  
  __suites__ = { }
  
  testSuites = [ case for case in dir( testComponent ) if case.startswith( 'Test_') ]

  print '    %d test suites found' % len( testSuites )
  
  for testSuite in testSuites:

    testSuiteName = testSuite
    testCaseModules = []
    
    testSuite = getattr( testComponent, testSuite ) 

    _fixture = None
    for testCaseName in dir( testSuite ):

      if testCaseName == '_fixture':
        _fixture = getattr( testSuite, testCaseName )
      elif testCaseName.startswith( 'Test_' ):
        testCaseModules.append( getattr( testSuite, testCaseName ) )

    if _fixture is not None:
      for testCaseModule in testCaseModules:
        testCaseModule.__bases__ = ( _fixture, )         

    __suites__[ testSuiteName ] = testCaseModules
    print '      %s' % testSuiteName

#  return testCaseModules
  return __suites__        

################################################################################
      
def runSuite( testSuiteName, testSuite ):

  print '\n%s' % separator
  print '  ---> %s ' % testSuiteName
  print separator

  loader = unittest.TestLoader()
  suite = None
  
  for testCase in testSuite:
    tests =  loader.loadTestsFromTestCase( testCase )

#    print '    %s' % test.__module__
#    _testSuiteTest = None
#    for testSuiteTest in testSuite._tests:
#      if _testSuiteTest is None:
#        _testSuiteTest = testSuiteTest.__class__.__name__
#        print '      %s' % _testSuiteTest
#      print '        %s' % testSuiteTest._testMethodName

    if suite is None:
      suite = tests
    else:
      suite.addTest( tests )    

  if suite is None:
    print '    0 testCases found'
    return 1
    
  print '    %d testCases found' % suite.countTestCases()

#  for testCase in suite._tests:
#
#    if isinstance( testCase, unittest.TestSuite ):
#      for testCase in testCase._tests:
##        print '      %s.%s' % ( testCase.__module__, testCase._testMethodName )
#        print '      %s.%s.%s' % ( testCase.__module__, testCase.__class__.__name__, testCase._testMethodName )
#    else: 
##      print '      %s.%s' % ( testCase.__module__, testCase._testMethodName )
#      print '      %s.%s.%s' % ( testCase.__module__, testCase.__class__.__name__, testCase._testMethodName )

  result = unittest.TextTestRunner(verbosity=2).run( suite )

################################################################################

def run():
  
  __testComponents__ = []

  for loader, module_name, is_pkg in  pkgutil.walk_packages( [ os.getcwd() ] ):   

    if module_name.startswith( 'Test_' ):
      try:
        module = loader.find_module(module_name).load_module(module_name)
        exec( '%s = module' % module_name )
      except Exception, e:
        print e
        continue  
 
      if is_pkg:
        __testComponents__.append( module_name )

  print '\n  %d test components found' % len( __testComponents__ )
  print separator
  for testComponentName in __testComponents__:
    print '    %s' % testComponentName     
  
  for testComponentName in __testComponents__:

    testComponent = loadTestComponent( testComponentName )
    if not testComponent:
      print '    test component cannot be loaded'
      print '    skipping test component "%s" ' % testComponentName
      continue

    testSuites = loadTestSuites( testComponent )
    if not testSuites:
#      print '    test suites not found'
      print '    skipping test component "%s" ' % testComponentName
      continue
    
    for testSuiteName, testSuite in testSuites.items():
      runSuite( testSuiteName, testSuite )
  
  print ''  

if __name__ == '__main__':
  run()

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF