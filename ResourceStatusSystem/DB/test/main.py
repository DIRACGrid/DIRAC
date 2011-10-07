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


'''
  Real code from here =)
'''

################################################################################
    
def loadSuite( testSuiteName ):
  
  suite = False
      
  print '\n----------------------------------------------------------------------'
  print '  %s ' % testSuiteName
  print '----------------------------------------------------------------------'
  print '    loading suite %s' % testSuiteName
  try:
    suite = __import__( testSuiteName )
    print '      ok'  
  except:
    pass   
    
  return suite  

################################################################################
    
def loadTestCases( suite ):
  
  print '    loading test cases %s' % testSuiteName  
  testCaseNames = [ case for case in dir( suite ) if case.startswith( 'TestCase_') ]
  
  return testCaseNames      

################################################################################

def loadTests( suite, testCaseName ):

  try:
    testCase = dynamicImport( testCaseName )
    print '      ok'
  except:
    return None

  testFound = [ test for test in dir( suite ) if test.startswith( testCaseName.replace( 'Case', '' ) ) ]

  print '      %d tests found' % len( testFound )
  
  tests = []
  for testName in testFound:
    try:
      test = dynamicImport( testName )
      tests.append( test )
      print '        %s loaded' % testName
    except:
      print '        error loading test "%s"' % testName
      
  return ( testCase, tests )    

################################################################################
      
def runSuite( testCase, tests ):

  import time

  class CustomTestRunner( unittest.TextTestRunner ):
           
    def run(self, test):
      "Run the given test case or test suite."
      result = self._makeResult()
      startTime = time.time()
      test(result)
      stopTime = time.time()
      timeTaken = stopTime - startTime
#      result.printErrors()
#      self.stream.writeln(result.separator2)
#      run = result.testsRun
#      self.stream.writeln("Ran %d test%s in %.3fs" %
#                          (run, run != 1 and "s" or "", timeTaken))
#      self.stream.writeln()
#      if not result.wasSuccessful():
#          self.stream.write("FAILED (")
#          failed, errored = map(len, (result.failures, result.errors))
#          if failed:
#            self.stream.write("failures=%d" % failed)
#          if errored:
#            if failed: self.stream.write(", ")
#            self.stream.write("errors=%d" % errored)
#          self.stream.writeln(")")
#      else:
#          self.stream.writeln("OK")
      return result

    

  print '    running !! \n'

  suite = unittest.defaultTestLoader.loadTestsFromTestCase( testCase )
  
  for test in tests:
    suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase( test ))
  
  result = CustomTestRunner( verbosity = 0 ).run( suite )
  
  print result.errors
  print result.failures
  
  result.printErrors()
  
#  result = unittest.TextTestRunner(verbosity=0).run( suite )
  #result.printErrorList(flavour, errors)
  
#  print "-------- --------- - - - - - - - -"
#  #print result
#  #print dir( result )
#  print result.errors
#  print result.failures
#  #print result.dots
#  print result.prin
#  #print result.testsRun    
#
#  print result.showAll
################################################################################

def dynamicImport( name ):
  
  m = __import__( name )
  return getattr( m, name )

################################################################################

if __name__ == '__main__':
  
  __suites__ = []

  for loader, module_name, is_pkg in  pkgutil.walk_packages( [ os.getcwd() ] ):   

    try:
      mod, name= module_name.split( '.' )
    
      if name.startswith( 'Test_' ) or name.startswith( 'TestCase_' ):
        module = loader.find_module(module_name).load_module(module_name)
        exec( '%s = module' % module_name )
       
    except Exception, e:
      pass

    if is_pkg and module_name.startswith( 'TestSuite_' ):
      try:
        module = loader.find_module(module_name).load_module(module_name)
        exec( '%s = module' % module_name )
        __suites__.append( module_name )
      except Exception, e:
        pass  

  print '\n  %d testSuites found' % len( __suites__ )
  print '----------------------------------------------------------------------'
  for testSuiteName in __suites__:
    print '    %s' % testSuiteName     
  
  
  for testSuiteName in __suites__:

    suite = loadSuite( testSuiteName )
    if not suite:
      print '    test suite cannot be loaded'
      print '    skipping test suite "%s" ' % testSuiteName
      continue

    testCaseNames = loadTestCases( suite )
    if not testCaseNames:
      print '    test case not found'
      print '    skipping test suite "%s" ' % testSuiteName
      continue
  
    for testCaseName in testCaseNames:
    
      testCase, tests = loadTests( suite, testCaseName )
      
      if tests is None:
        print '    test suite cannot be loaded'
        print '    skipping test suite "%s" ' % testSuiteName
            
      if not tests:
        print '    no test loaded'
        print '    skipping test suite "%s" ' % testSuiteName
        continue
  
      runSuite( testCase, tests )
  
print ''  