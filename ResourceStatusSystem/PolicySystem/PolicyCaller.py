# $HeadURL $
''' PolicyCaller

  Module used for calling policies. Its class is used for invoking
  real policies, based on the policy name.
  
'''

from DIRAC                                            import gLogger, S_ERROR
from DIRAC.ResourceStatusSystem.Utilities             import Utils
from DIRAC.ResourceStatusSystem.Command.CommandCaller import CommandCaller

__RCSID__  = '$Id: $'

class PolicyCaller:
  '''
    PolicyCaller loads policies, sets commands and runs them.
  '''
  
  def __init__( self, clients = None ):
    '''
      Constructor
    '''

    self.cCaller = CommandCaller()  
    
    self.clients = {}
    if clients is not None: 
      self.clients = clients       

  def policyInvocation( self, decissionParams, policyDict ):
#  def policyInvocation( self, granularity = None, name = None,
#                        status = None, policy = None, args = None, pName = None,
#                        pModule = None, extraArgs = None, commandIn = None ):    
    '''
    Invokes a policy:

    1. If :attr:`policy` is None, import the policy module specified
    with :attr:`pModule` (e.g. 'DT_Policy').

      1.1. Create a policy object.

    2. Set the policy arguments (usually :attr:`granularity`,
    :attr:`name`) + :attr:`extraArgs`.

    3. If commandIn is specified (normally it is), use
    :meth:`DIRAC.ResourceStatusSystem.Command.CommandCaller.CommandCaller.setCommandObject`
    to get a command object
    '''

    # policyDict.keys() = [ 'description', 'module', 'commandIn', 'args'... ]    

#    print policyDict[ 'description' ]
#    print policyDict[ 'module' ]
#    print policyDict[ 'command' ]
#    print policyDict[ 'args' ]

    if not 'module' in policyDict:
      return S_ERROR( 'Malformed policyDict %s' % policyDict )
    pModuleName = policyDict[ 'module' ]

    if not 'command' in policyDict:
      return S_ERROR( 'Malformed policyDict %s' % policyDict )
    pCommand = policyDict[ 'command' ]
    
    if not 'args' in policyDict:
      return S_ERROR( 'Malformed policyDict %s' % policyDict )
    pArgs = policyDict[ 'args' ]

    try:     
      policyModule = Utils.voimport( 'DIRAC.ResourceStatusSystem.Policy.%s' % pModuleName )
    except ImportError:
      return S_ERROR( 'Unable to import DIRAC.ResourceStatusSystem.Policy.%s' % pModuleName )
    
    if not hasattr( policyModule, pModuleName ):
      return S_ERROR( '%s has no attibute %s' % ( policyModule, pModuleName ) ) 
    
    policy  = getattr( policyModule, pModuleName )() 
    
    command = self.cCaller.commandInvocation( pCommand, pArgs, decissionParams, self.clients )
    if not command[ 'OK' ]:
      return command
    command = command[ 'Value' ]

#    res[ 'PolicyName' ] = pName
#    return res
    
    evaluationResult = self.policyEvaluation( policy, command )
    
    return evaluationResult

################################################################################

#    try:
#      
#      policyModule = Utils.voimport( 'DIRAC.ResourceStatusSystem.Policy.%s' % policyModule )
#        
#    except ImportError:
#      _msg = 'Unable to import a policy module named %s, falling back on AlwaysFalse_Policy.' % pModule
#      gLogger.warn( _msg )
#      policyModule = __import__( 'DIRAC.ResourceStatusSystem.Policy.AlwaysFalse_Policy',
#                                   globals(), locals(), ['*'] )
#      pModule = 'AlwaysFalse_Policy'
#        
#    try:
#        
#      policy = getattr( policyModule, pModule )()
#        
#    except AttributeError as exc:
#      print policyModule, pModule
#      raise exc
#
#    if not args:
#      args = ( granularity, name )
#
#    if extraArgs:
#      args = args + tuple( extraArgs )
#
#    if commandIn:
#      commandIn = self.cCaller.setCommandObject( commandIn )
#      for clientName, clientInstance in self.clients.items():
#        
#        self.cCaller.setAPI( commandIn, clientName, clientInstance )
#    res = self._innerEval( policy, args, commandIn = commandIn )
#    # Just adding the PolicyName to the result of the evaluation of the policy
#    res[ 'PolicyName' ] = pName
#    return res

  def policyEvaluation( self, policy, command ):#, pArgs, decissionParams ):
    
#    for clientName, clientInstance in self.clients.items():
#      
#      command.setAPI( clientName, clientInstance )

#    command.setDecissionParams( decissionParams )
#    command.setArgs( pArgs )

    policy.setCommand( command )

    _evaluationResult = self._policyEvaluation( policy )
    
    return _evaluationResult    

  def _policyEvaluation( self, policy ):#decissionParams, policyArguments, policyCommand ):
    '''
      Policy evaluation
    '''
    
    #policy.setArgs( policyArguments )
    #policy.setDecissionParams( decissionParams )
    #policy.setCommand( policyCommand )
    
    return policy.evaluate()

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF