""" The Script class provides a simple way for users to specify an executable
    or file to run (and is also a simple example of a workflow module).
"""

import os, sys, re

from DIRAC.Core.Utilities.Subprocess    import shellCall
from DIRAC                              import gLogger
from DIRAC.Core.Utilities.Os            import which

from DIRAC.Workflow.Modules.ModuleBase  import ModuleBase

class Script( ModuleBase ):

  #############################################################################
  def __init__( self ):
    """ c'tor
    """
    self.log = gLogger.getSubLogger( 'Script' )
    super( Script, self ).__init__( self.log )

    # Set defaults for all workflow parameters here
    self.executable = ''
    self.applicationName = ''
    self.applicationVersion = ''
    self.applicationLog = ''
    self.arguments = ''
    self.step_commons = {}

    self.environment = None
    self.callbackFunction = None
    self.bufferLimit = 52428800

  #############################################################################

  def _resolveInputVariables( self ):
    """ By convention the workflow parameters are resolved here.
    """
    super( Script, self )._resolveInputVariables()
    super( Script, self )._resolveInputStep()

    if self.step_commons.has_key( 'arguments' ):
      self.arguments = self.step_commons['arguments']

  #############################################################################

  def _initialize( self ):
    """ simple checks
    """
    if not self.executable:
      raise RuntimeError, 'No executable defined'

  def _setCommand( self ):
    """ set the command that will be executed
    """
    self.command = self.executable
    if os.path.exists( os.path.basename( self.executable ) ):
      self.executable = os.path.basename( self.executable )
      if not os.access( '%s/%s' % ( os.getcwd(), self.executable ), 5 ):
        os.chmod( '%s/%s' % ( os.getcwd(), self.executable ), 0755 )
      self.command = '%s/%s' % ( os.getcwd(), self.executable )
    elif re.search( '.py$', self.executable ):
      self.command = '%s %s' % ( sys.executable, self.executable )
    elif which( self.executable ):
      self.command = self.executable

    if self.arguments:
      self.command = '%s %s' % ( self.command, self.arguments )

    self.log.info( 'Command is: %s' % self.command )

  def _executeCommand( self ):
    """ execute the self.command (uses shellCall)
    """
    failed = False

    outputDict = shellCall( 0, self.command,
                            env = self.environment,
                            callbackFunction = self.callbackFunction,
                            bufferLimit = self.bufferLimit )
    if not outputDict['OK']:
      failed = True
      self.log.error( 'Shell call execution failed:' )
      self.log.error( outputDict['Message'] )
    status, stdout, stderr = outputDict['Value'][0:3]
    if status:
      failed = True
      self.log.error( "Non-zero status %s while executing %s" % ( status, self.command ) )
    else:
      self.log.info( "%s execution completed with status %s" % ( self.executable, status ) )

    self.log.verbose( stdout )
    self.log.verbose( stderr )
    if os.path.exists( self.applicationLog ):
      self.log.verbose( 'Removing existing %s' % self.applicationLog )
      os.remove( self.applicationLog )
    fopen = open( '%s/%s' % ( os.getcwd(), self.applicationLog ), 'w' )
    fopen.write( "<<<<<<<<<< %s Standard Output >>>>>>>>>>\n\n%s " % ( self.executable, stdout ) )
    if stderr:
      fopen.write( "<<<<<<<<<< %s Standard Error >>>>>>>>>>\n\n%s " % ( self.executable, stderr ) )
    fopen.close()
    self.log.info( "Output written to %s, execution complete." % ( self.applicationLog ) )

    if failed:
      raise RuntimeError, "'%s' Exited With Status %s" % ( os.path.basename( self.executable ), status )


  def _finalize( self ):
    """ simply finalize
    """
    status = "%s (%s %s) Successful" % ( os.path.basename( self.executable ),
                                         self.applicationName,
                                         self.applicationVersion )

    super( Script, self )._finalize( status )
